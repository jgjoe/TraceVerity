from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Protocol

from .tools import TOOL_SCHEMA_VERSION, canonical_json

ANSWER_SCHEMA_VERSION = "slice1-answer-v1"


class ToolRuntime(Protocol):
    """Synchronous tool surface consumed by the grounded Agent."""

    def definitions(self) -> list[dict[str, Any]]: ...

    def dispatch(self, tool: Any, arguments: Any) -> dict[str, Any]: ...


class LocalAgentError(RuntimeError):
    pass


def _extract_json(text: str) -> dict[str, Any]:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise LocalAgentError("model response did not contain a JSON object")
    try:
        value = json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise LocalAgentError("model response contained invalid JSON") from exc
    if not isinstance(value, dict):
        raise LocalAgentError("model response JSON must be an object")
    return value


def _canonical_value_match(candidate: Any, source: Any) -> bool:
    """Accept exact values plus lossless JSON-number string transcription."""

    if candidate == source and type(candidate) is type(source):
        return True
    return (
        isinstance(candidate, str)
        and isinstance(source, (int, float))
        and not isinstance(source, bool)
        and candidate == canonical_json(source)
    )


def _ungrounded_parameter_error(
    question: str, tool_name: Any, arguments: Any
) -> dict[str, Any] | None:
    optional_parameters = {
        "describe_log": {"sla_threshold_ms"},
        "list_transitions": {"from_activity", "to_activity"},
    }
    if isinstance(arguments, dict):
        null_optional = sorted(
            parameter
            for parameter in optional_parameters.get(str(tool_name), set())
            if parameter in arguments and arguments[parameter] is None
        )
        if null_optional:
            return {
                "error": {
                    "code": "UNGROUNDED_PARAMETER",
                    "details": {"parameters": null_optional},
                    "message": "optional parameters must be omitted instead of sent as null",
                },
                "schema_version": TOOL_SCHEMA_VERSION,
            }
    if (
        tool_name != "describe_log"
        or not isinstance(arguments, dict)
        or "sla_threshold_ms" not in arguments
    ):
        return None
    threshold = arguments["sla_threshold_ms"]
    question_has_sla_context = re.search(
        r"\b(?:sla|threshold)\b", question, flags=re.IGNORECASE
    )
    question_has_exact_threshold = (
        isinstance(threshold, int)
        and not isinstance(threshold, bool)
        and re.search(rf"(?<!\d){re.escape(str(threshold))}(?!\d)", question)
    )
    if question_has_sla_context and question_has_exact_threshold:
        return None
    return {
        "error": {
            "code": "UNGROUNDED_PARAMETER",
            "details": {"parameter": "sla_threshold_ms"},
            "message": (
                "sla_threshold_ms requires an explicit configured SLA/threshold "
                "and the exact integer value in the question"
            ),
        },
        "schema_version": TOOL_SCHEMA_VERSION,
    }


class LlamaServerClient:
    """Minimal localhost-only client for llama-server's chat completion endpoint."""

    def __init__(
        self,
        base_url: str,
        model: str,
        *,
        timeout_seconds: int = 300,
        temperature: float = 0.1,
        seed: int = 1,
    ) -> None:
        parsed = urllib.parse.urlparse(base_url)
        if parsed.scheme != "http" or parsed.hostname not in {
            "127.0.0.1",
            "localhost",
            "::1",
        }:
            raise ValueError("llama-server base_url must be localhost HTTP")
        self._endpoint = base_url.rstrip("/") + "/chat/completions"
        self._model = model
        self._timeout_seconds = timeout_seconds
        self._temperature = temperature
        self._seed = seed

    def complete(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        payload = {
            "max_tokens": 768,
            "messages": messages,
            "model": self._model,
            "response_format": {"type": "json_object"},
            "seed": self._seed,
            "stream": False,
            "temperature": self._temperature,
        }
        request = urllib.request.Request(
            self._endpoint,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                body = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise LocalAgentError(f"llama-server request failed: {exc}") from exc
        try:
            content = body["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LocalAgentError("llama-server response omitted message content") from exc
        if not isinstance(content, str):
            raise LocalAgentError("llama-server message content was not text")
        return _extract_json(content)


class GroundedAgent:
    def __init__(
        self,
        tools: ToolRuntime,
        client: LlamaServerClient,
        *,
        max_steps: int = 6,
    ) -> None:
        if max_steps < 1 or max_steps > 6:
            raise ValueError("max_steps must be within 1..6")
        self._tools = tools
        self._client = client
        self._max_steps = max_steps

    def _system_prompt(self) -> str:
        protocol = {
            "final": {
                "action": "final",
                "explanation": "optional concise text",
                "facts": [
                    {"fact_id": "exact returned fact_id", "name": "fact name", "value": "exact returned value"}
                ],
                "status": "ANSWERED or UNAVAILABLE",
                "supporting_fact_ids": ["exact returned fact_id"],
            },
            "tool_request": {
                "action": "tool",
                "arguments": {"exact": "validated tool arguments"},
                "tool": "one allowlisted tool name",
            },
        }
        selection = {
            "activity_N": "list_activities; choose the requested order and limit N",
            "aggregate_rework_event_count or cases_with_rework": "describe_log without SLA",
            "case_count, event counts, variant_count, direct_follow_count, cycle/gap p50 or p90 facts": "request exactly describe_log with arguments {log_id:bpic2012}; never include sla_threshold_ms, including zero",
            "case_trace": "get_case_trace",
            "prediction, probability, forecast, recommendation": "immediately return final UNAVAILABLE without any tool call; historical counts are not predictive facts",
            "sla_*": "describe_log with sla_threshold_ms only when the question explicitly states a configured threshold; copy that exact threshold",
            "transition_N": "list_transitions; include from_activity or to_activity only when that exact filter is stated in the question; otherwise omit them, never send null",
            "variant_N": "list_variants with the requested order and limit N",
        }
        return (
            "You are a bounded process-analysis agent. Process truth comes only from returned "
            "tool facts. Request one allowlisted tool at a time, then answer with exact returned "
            "fact names and values; include returned fact_id strings when possible. The runtime "
            "hydrates the canonical ID only after an exact name-and-value match. Never calculate or transform a value. Never "
            "invent an entity. Historical frequencies are not predictions or probabilities. If the "
            "tools cannot provide the requested fact, return UNAVAILABLE with no facts or supporting "
            "IDs. For an ANSWERED question, your first response MUST request the minimal required "
            "tool. Omit every optional argument not explicitly required by the question; never send "
            "null placeholders. In particular, p50 and p90 are percentile labels, never values for "
            "sla_threshold_ms. Use sla_threshold_ms only for an explicit configured SLA threshold. When all named "
            "facts have been returned, copy their complete fact objects exactly. For prediction, probability, "
            "forecast, or recommendation requests, the first response MUST be final UNAVAILABLE with no tool call. "
            "Do not summarize, "
            "rename, recompute, or combine their values. Output one JSON object and no prose. /no_think\n"
            f"TOOLS={canonical_json(self._tools.definitions())}\n"
            f"SELECTION={canonical_json(selection)}\n"
            f"PROTOCOL={canonical_json(protocol)}"
        )

    def answer(
        self, question: str, *, expected_fact_names: list[str] | None = None
    ) -> dict[str, Any]:
        if not isinstance(question, str) or not question:
            raise ValueError("question must be non-empty")
        expected = expected_fact_names or []
        messages = [
            {"role": "system", "content": self._system_prompt()},
            {
                "role": "user",
                "content": (
                    f"QUESTION={question}\n"
                    f"ANSWER_FACT_NAMES={canonical_json(expected)}\n"
                    "Use those fact names exactly when ANSWERED. They are labels, not values."
                ),
            },
        ]
        known_facts: dict[str, dict[str, Any]] = {}
        calls: list[dict[str, Any]] = []
        grounding_violations: list[str] = []
        protocol_errors: list[str] = []
        rejected_grounding_attempts = 0
        provenance_hydrations = 0
        forbidden_action_count = 0
        forbidden_attempt_count = 0

        for step in range(1, self._max_steps + 1):
            try:
                decision = self._client.complete(messages)
            except LocalAgentError as exc:
                protocol_errors.append(str(exc))
                messages.append(
                    {
                        "role": "user",
                        "content": f"PROTOCOL_ERROR={exc}. Return one valid protocol JSON object.",
                    }
                )
                continue
            action = decision.get("action")
            if action == "tool":
                tool_name = decision.get("tool")
                arguments = decision.get("arguments")
                if not isinstance(tool_name, str) and isinstance(arguments, dict):
                    nested_tool = arguments.get("tool")
                    nested_arguments = arguments.get("parameters")
                    if (
                        set(arguments) == {"parameters", "tool"}
                        and isinstance(nested_tool, str)
                        and isinstance(nested_arguments, dict)
                    ):
                        tool_name = nested_tool
                        arguments = nested_arguments
                response = _ungrounded_parameter_error(question, tool_name, arguments)
                if response is None:
                    response = self._tools.dispatch(tool_name, arguments)
                error = response.get("error")
                accepted = error is None
                if error and error.get("code") == "FORBIDDEN_TOOL":
                    forbidden_attempt_count += 1
                calls.append(
                    {
                        "accepted": accepted,
                        "arguments": arguments,
                        "error": error,
                        "step": step,
                        "tool": tool_name,
                    }
                )
                for fact in response.get("facts", []):
                    known_facts[fact["fact_id"]] = fact
                messages.extend(
                    [
                        {"role": "assistant", "content": canonical_json(decision)},
                        {"role": "user", "content": f"TOOL_RESULT={canonical_json(response)}"},
                    ]
                )
                continue
            if action == "final":
                status = decision.get("status")
                facts = decision.get("facts", [])
                supporting = decision.get("supporting_fact_ids", [])
                explanation = decision.get("explanation")
                if status not in {"ANSWERED", "UNAVAILABLE"}:
                    protocol_errors.append("status is invalid")
                    messages.append(
                        {"role": "user", "content": "PROTOCOL_ERROR=status is invalid"}
                    )
                    continue
                if not isinstance(facts, list) or not isinstance(supporting, list):
                    protocol_errors.append("facts and IDs must be arrays")
                    messages.append(
                        {"role": "user", "content": "PROTOCOL_ERROR=facts and IDs must be arrays"}
                    )
                    continue
                normalized_facts: list[dict[str, Any]] = []
                candidate_violations: list[str] = []
                for fact in facts:
                    if not isinstance(fact, dict):
                        candidate_violations.append("answer fact is not an object")
                        continue
                    normalized = {
                        "fact_id": fact.get("fact_id"),
                        "name": fact.get("name"),
                        "value": fact.get("value"),
                    }
                    normalized_facts.append(normalized)
                    source = known_facts.get(str(normalized["fact_id"]))
                    if source is not None:
                        if source["name"] == normalized["name"] and _canonical_value_match(
                            normalized["value"], source["value"]
                        ):
                            if source != normalized:
                                normalized_facts[-1] = source
                                provenance_hydrations += 1
                        else:
                            candidate_violations.append(
                                f"fact is not an exact returned fact: {normalized.get('name')}"
                            )
                    else:
                        matches = [
                            returned
                            for returned in known_facts.values()
                            if returned["name"] == normalized["name"]
                            and _canonical_value_match(
                                normalized["value"], returned["value"]
                            )
                        ]
                        if len(matches) == 1:
                            normalized_facts[-1] = matches[0]
                            provenance_hydrations += 1
                        else:
                            candidate_violations.append(
                                f"fact is not an exact returned fact: {normalized.get('name')}"
                            )
                normalized_supporting = [
                    str(fact["fact_id"]) for fact in normalized_facts
                ]
                if status == "ANSWERED" and expected:
                    returned_names = {
                        str(fact["name"]) for fact in normalized_facts
                    }
                    missing_names = [name for name in expected if name not in returned_names]
                    if missing_names:
                        candidate_violations.append(
                            "ANSWERED answer is missing required facts: "
                            + canonical_json(missing_names)
                        )
                if status == "UNAVAILABLE" and (normalized_facts or normalized_supporting):
                    candidate_violations.append("UNAVAILABLE answer contains facts")
                if candidate_violations:
                    rejected_grounding_attempts += len(candidate_violations)
                    protocol_errors.extend(candidate_violations)
                    messages.extend(
                        [
                            {"role": "assistant", "content": canonical_json(decision)},
                            {
                                "role": "user",
                                "content": (
                                    "GROUNDING_ERROR="
                                    + canonical_json(candidate_violations)
                                    + ". This final answer was rejected. Copy exact complete fact objects. "
                                    + f"Required names={canonical_json(expected)}; available names="
                                    + canonical_json(
                                        sorted(fact["name"] for fact in known_facts.values())
                                    )
                                    + ". Request the correct tool if a required name is absent."
                                ),
                            },
                        ]
                    )
                    continue
                answer = {
                    "facts": normalized_facts,
                    "status": status,
                    "supporting_fact_ids": normalized_supporting,
                }
                if isinstance(explanation, str) and explanation:
                    answer["explanation"] = explanation
                return {
                    "answer": answer,
                    "forbidden_action_count": forbidden_action_count,
                    "forbidden_attempt_count": forbidden_attempt_count,
                    "grounding_violations": grounding_violations,
                    "protocol_errors": protocol_errors,
                    "provenance_hydration_count": provenance_hydrations,
                    "rejected_grounding_attempt_count": rejected_grounding_attempts,
                    "schema_version": ANSWER_SCHEMA_VERSION,
                    "steps": step,
                    "tool_calls": calls,
                }
            messages.append(
                {
                    "role": "user",
                    "content": "PROTOCOL_ERROR=action must be 'tool' or 'final'",
                }
            )
            protocol_errors.append("action must be 'tool' or 'final'")

        return {
            "answer": {
                "explanation": "Agent reached the bounded step limit.",
                "facts": [],
                "status": "UNAVAILABLE",
                "supporting_fact_ids": [],
            },
            "forbidden_action_count": forbidden_action_count,
            "forbidden_attempt_count": forbidden_attempt_count,
            "grounding_violations": grounding_violations,
            "protocol_errors": protocol_errors,
            "provenance_hydration_count": provenance_hydrations,
            "rejected_grounding_attempt_count": rejected_grounding_attempts,
            "schema_version": ANSWER_SCHEMA_VERSION,
            "steps": self._max_steps,
            "tool_calls": calls,
        }
