from __future__ import annotations

import hashlib
import json
import re
from textwrap import dedent

from openai import AsyncOpenAI

from app.cache import TTLCache
from app.config import Settings
from app.models import ChunkExtraction, OutputEntity, ScrapedPage
from app.persistent_cache import PersistentCache


SYSTEM_PROMPT = dedent(
    """
    You extract structured entities from web content.

    Return JSON only.
    The top-level JSON must be an object with this exact shape:
    {
      "entities": [
        {
          "name": "string",
          "entity_type": "string",
          "summary": "string",
          "homepage": "string or null",
          "relevance_score": 0.0,
          "cells": [
            {
              "field_name": "snake_case_string",
              "value": "string",
              "confidence": 0.0,
              "evidence": {
                "source_url": "string",
                "source_title": "string",
                "quote": "short verbatim quote"
              }
            }
          ]
        }
      ]
    }

    Rules:
    - Only extract entities clearly relevant to the user's topic query.
    - Only include facts directly supported by the provided page text or snippet.
    - Every field must be traceable to a supporting quote from the provided source.
    - Use short snake_case field_name values for attributes.
    - Do not invent fields or values. Omit uncertain data.
    - Prefer entity attributes that help a human compare options for this query.
    - homepage should only be filled when the page directly supports it, or when the page itself is the entity's clearly official homepage.
    - Keep summaries concise and grounded.
    - Keep evidence.quote under 18 words.
    - Include only the 4 most useful attributes per entity.
    - Return no duplicate entities within the same chunk.
    - If nothing relevant is found, return {"entities": []}.
    """
).strip()


class LLMExtractor:
    namespace = "extract"

    def __init__(self, settings: Settings, persistent_cache: PersistentCache | None = None):
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required")

        client_kwargs = {
            "api_key": settings.openai_api_key,
            "timeout": settings.request_timeout_seconds,
        }
        if getattr(settings, "openai_base_url", None):
            client_kwargs["base_url"] = settings.openai_base_url

        self.settings = settings
        self.client = AsyncOpenAI(**client_kwargs)
        self.persistent_cache = persistent_cache
        self.cache = TTLCache[dict](
            maxsize=settings.max_cached_extractions,
            ttl_seconds=settings.extraction_cache_ttl_seconds,
        )
        self.verify_cache = TTLCache[dict](
            maxsize=settings.max_cached_extractions,
            ttl_seconds=settings.extraction_cache_ttl_seconds,
        )

    async def close(self) -> None:
        await self.client.close()

    def _strip_code_fences(self, text: str) -> str:
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
        return text.strip()

    def _coerce_payload(self, payload: object) -> dict:
        if isinstance(payload, list):
            return {"entities": payload}

        if isinstance(payload, dict):
            if "entities" in payload and isinstance(payload["entities"], list):
                return payload
            for alt_key in ("results", "items", "data"):
                if alt_key in payload and isinstance(payload[alt_key], list):
                    return {"entities": payload[alt_key]}

        return {"entities": []}

    def _cache_key(self, query: str, page: ScrapedPage, chunk: str) -> str:
        chunk_hash = hashlib.sha1(chunk.encode("utf-8")).hexdigest()
        payload = json.dumps(
            {
                "model": self.settings.openai_model,
                "query": query.strip().lower(),
                "url": page.url.strip().lower(),
                "chunk": chunk_hash,
            },
            sort_keys=True,
        )
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    def _verify_cache_key(self, query: str, query_type: str, rows: list[OutputEntity]) -> str:
        payload = {
            "model": self.settings.openai_model,
            "query": query.strip().lower(),
            "query_type": query_type,
            "rows": [
                {
                    "entity_id": row.entity_id,
                    "name": row.name.value,
                    "summary": row.summary.value,
                    "entity_type": row.entity_type.value,
                    "sources": [source.model_dump() for source in row.name.sources[:3]],
                    "attributes": {key: value.value for key, value in sorted(row.attributes.items())},
                }
                for row in rows
            ],
        }
        return hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

    def _get_cached(self, cache_key: str) -> ChunkExtraction | None:
        cached = self.cache.get(cache_key)
        if cached is not None:
            return ChunkExtraction.model_validate(cached)

        if self.persistent_cache is None:
            return None

        payload = self.persistent_cache.get(self.namespace, cache_key)
        if payload is None:
            return None

        extraction = ChunkExtraction.model_validate(payload)
        self.cache.set(cache_key, extraction.model_dump())
        return extraction

    def _store_cached(self, cache_key: str, extraction: ChunkExtraction) -> ChunkExtraction:
        payload = extraction.model_dump()
        self.cache.set(cache_key, payload)
        if self.persistent_cache is not None:
            self.persistent_cache.set(
                self.namespace,
                cache_key,
                payload,
                self.settings.extraction_cache_ttl_seconds,
            )
        return extraction

    async def _request_extraction(self, user_prompt: str) -> ChunkExtraction:
        completion = await self.client.chat.completions.create(
            model=self.settings.openai_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0,
            max_tokens=self.settings.max_extraction_completion_tokens,
            response_format={"type": "json_object"},
        )

        content = completion.choices[0].message.content or '{"entities": []}'
        content = self._strip_code_fences(content)
        payload = json.loads(content)
        payload = self._coerce_payload(payload)
        return ChunkExtraction.model_validate(payload)

    async def extract_from_chunk(
        self,
        query: str,
        page: ScrapedPage,
        chunk: str,
        schema_fields: list[str] | None = None,
    ) -> ChunkExtraction:
        cache_key = self._cache_key(query, page, chunk)
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        entity_limit = min(self.settings.max_entities_per_chunk, 4)
        user_prompt = dedent(
            f"""
            Topic query:
            {query}

            Preferred comparison fields:
            {", ".join(schema_fields or []) or "(infer the best fields)"}

            Source page title:
            {page.title}

            Source page URL:
            {page.url}

            Search snippet:
            {page.snippet or '(none)'}

            Page chunk:
            {chunk}

            Extract up to {entity_limit} relevant entities from this source.
            For each extracted cell, set evidence.source_url to the exact source URL above and
            evidence.source_title to the source page title above.
            Evidence.quote must be a short verbatim quote from the provided snippet or page chunk
            that supports the value.
            """
        ).strip()

        try:
            extraction = await self._request_extraction(user_prompt)
            return self._store_cached(cache_key, extraction)

        except Exception as exc:
            fallback_chunk = chunk[: max(1200, len(chunk) // 2)].strip()
            fallback_prompt = dedent(
                f"""
                Topic query:
                {query}

                Source page title:
                {page.title}

                Source page URL:
                {page.url}

                Search snippet:
                {page.snippet or '(none)'}

                Page chunk:
                {fallback_chunk}

                Return compact JSON only.
                Extract up to {max(2, entity_limit // 2)} relevant entities from this source.
                Keep evidence.quote very short and include at most 3 attributes per entity.
                """
            ).strip()
            try:
                extraction = await self._request_extraction(fallback_prompt)
                return self._store_cached(cache_key, extraction)
            except Exception as fallback_exc:
                print(f"[extract_from_chunk] extraction failed for {page.url}: {exc}")
                print(f"[extract_from_chunk] fallback failed for {page.url}: {fallback_exc}")
                return ChunkExtraction(entities=[])

    async def verify_rows(
        self,
        query: str,
        query_type: str,
        rows: list[OutputEntity],
    ) -> dict[str, dict[str, object]]:
        if not rows:
            return {}

        cache_key = self._verify_cache_key(query, query_type, rows)
        cached = self.verify_cache.get(cache_key)
        if cached is not None:
            return dict(cached)
        if self.persistent_cache is not None:
            payload = self.persistent_cache.get(f"{self.namespace}:verify", cache_key)
            if payload is not None:
                self.verify_cache.set(cache_key, payload)
                return dict(payload)

        prompt_rows = []
        for row in rows:
            prompt_rows.append(
                {
                    "entity_id": row.entity_id,
                    "name": row.name.value,
                    "entity_type": row.entity_type.value,
                    "summary": row.summary.value,
                    "homepage": row.homepage.value if row.homepage else "",
                    "attributes": {key: value.value for key, value in row.attributes.items()},
                    "evidence": [
                        {
                            "source_url": source.source_url,
                            "source_title": source.source_title,
                            "quote": source.quote,
                        }
                        for source in row.name.sources[:3]
                    ],
                }
            )

        verification_prompt = dedent(
            f"""
            Validate candidate entities for the topic query below.
            Return JSON only in this exact shape:
            {{
              "items": [
                {{
                  "entity_id": "string",
                  "verification_status": "verified|plausible|weak",
                  "verification_score": 0.0,
                  "reason": "short reason"
                }}
              ]
            }}

            Topic query:
            {query}

            Query type:
            {query_type}

            Candidate rows:
            {json.dumps(prompt_rows, ensure_ascii=True)}
            """
        ).strip()

        try:
            completion = await self.client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {"role": "system", "content": "You validate extracted entities against evidence. Return JSON only."},
                    {"role": "user", "content": verification_prompt},
                ],
                temperature=0,
                max_tokens=900,
                response_format={"type": "json_object"},
            )
            content = completion.choices[0].message.content or '{"items": []}'
            content = self._strip_code_fences(content)
            payload = json.loads(content)
            items = payload.get("items", []) if isinstance(payload, dict) else []
            result = {
                item.get("entity_id", ""): {
                    "verification_status": item.get("verification_status", "unverified"),
                    "verification_score": float(item.get("verification_score", 0.0) or 0.0),
                    "reason": item.get("reason", ""),
                }
                for item in items
                if isinstance(item, dict) and item.get("entity_id")
            }
        except Exception:
            result = {}

        self.verify_cache.set(cache_key, result)
        if self.persistent_cache is not None:
            self.persistent_cache.set(
                f"{self.namespace}:verify",
                cache_key,
                result,
                self.settings.extraction_cache_ttl_seconds,
            )
        return result
