"""Provider-agnostic LLM client.

    LAKESENSE_LLM_PROVIDER = mock | openai | anthropic | bedrock   (default: mock)
    LAKESENSE_LLM_MODEL    = model id override for the chosen provider

The `mock` provider is deterministic and needs no API key, so the project runs
offline, in CI, and for any recruiter who clones it. Real providers are imported
lazily, so their SDKs are optional dependencies.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Any

from lakesense.logging_utils import get_logger

log = get_logger(__name__)


class LLMClient(ABC):
    name = "base"

    @abstractmethod
    def complete(self, system: str, prompt: str, *, task: str = "", context: dict[str, Any] | None = None) -> str:
        """Return the model's text answer.

        `task` and `context` carry structured info. Real models only need the
        prompt; the mock uses them to answer deterministically.
        """


class MockLLM(LLMClient):
    """Rule-based stand-in that imitates what an LLM would return for each task."""

    name = "mock"

    def complete(self, system, prompt, *, task="", context=None):
        from lakesense.ai import mock_responses

        context = context or {}
        handler = {
            "dq_explain": mock_responses.dq_explanation,
            "anomaly_explain": mock_responses.anomaly_explanation,
            "text_to_sql": mock_responses.text_to_sql,
            "summarize_result": mock_responses.summarize_result,
        }.get(task)
        if handler is None:
            return "Mock LLM: no handler for this task."
        return handler(context)


class OpenAILLM(LLMClient):
    name = "openai"

    def __init__(self, model: str | None = None):
        from openai import OpenAI  # optional dependency

        self.client = OpenAI()
        self.model = model or "gpt-4o-mini"

    def complete(self, system, prompt, *, task="", context=None):
        resp = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content or ""


class AnthropicLLM(LLMClient):
    name = "anthropic"

    def __init__(self, model: str | None = None):
        import anthropic  # optional dependency

        self.client = anthropic.Anthropic()
        self.model = model or "claude-sonnet-4-5"

    def complete(self, system, prompt, *, task="", context=None):
        resp = self.client.messages.create(
            model=self.model, max_tokens=1500, temperature=0, system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        return "".join(b.text for b in resp.content if getattr(b, "type", "") == "text")


class BedrockLLM(LLMClient):
    """Amazon Bedrock via the Converse API (works for Claude, Llama, Mistral, Titan...)."""

    name = "bedrock"

    def __init__(self, model: str | None = None):
        import boto3  # optional dependency

        self.client = boto3.client("bedrock-runtime", region_name=os.getenv("AWS_REGION", "us-east-1"))
        self.model = model or "anthropic.claude-3-5-sonnet-20240620-v1:0"

    def complete(self, system, prompt, *, task="", context=None):
        resp = self.client.converse(
            modelId=self.model,
            system=[{"text": system}],
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"temperature": 0, "maxTokens": 1500},
        )
        return resp["output"]["message"]["content"][0]["text"]


PROVIDERS = {"mock": MockLLM, "openai": OpenAILLM, "anthropic": AnthropicLLM, "bedrock": BedrockLLM}


def get_llm(provider: str | None = None) -> LLMClient:
    provider = (provider or os.getenv("LAKESENSE_LLM_PROVIDER", "mock")).lower()
    if provider not in PROVIDERS:
        raise ValueError(f"Unknown LLM provider '{provider}'. Choose from {sorted(PROVIDERS)}")
    cls = PROVIDERS[provider]
    client = cls() if cls is MockLLM else cls(os.getenv("LAKESENSE_LLM_MODEL"))
    log.info("LLM provider: %s", client.name)
    return client
