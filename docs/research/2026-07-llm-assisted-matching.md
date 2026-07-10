# LLM-Assisted Matching for the Brazilian Polling-Station Geocoder — Research Findings

**Date:** 2026-07-10
**Purpose (plain language):** We geocode ~400k Brazilian polling stations by matching messy
Portuguese addresses and school names against census reference data, then a LightGBM model picks the
best candidate. This document asks a narrow engineering question: **would adding a large language
model (LLM) anywhere in that process actually help, given that the production pipeline must stay
free/open and fully reproducible?** It answers per use case (normalization, tie-breaking, candidate
generation), grounds each answer in primary sources (peer-reviewed / arXiv papers, official model
cards and license texts, first-party inference-engine docs), and separates *demonstrated in a paper*
from *plausible*.

This feeds a go/no-go decision. It is deliberately skeptical.

---

## 0. TL;DR — go/no-go per use case

| Use case | Verdict | One-line rationale | Evidence strength |
|---|---|---|---|
| **(a) Normalization / abbreviation expansion** | **Conditional GO — offline dictionary expansion only. NO to per-row production inference.** | An LLM's realistic value is a one-time, human-reviewed expansion of the ~60-entry school-synonym list / a gazetteer, committed to the repo and used deterministically. Per-row LLM normalization buys little over extending the deterministic list and imports hallucination + reproducibility risk. | Moderate (address-parsing papers show competence *and* fabrication) |
| **(b) Hard-case tie-breaking / re-ranking ambiguous candidates** | **Conditional GO — but only as an offline evaluation and *after* cheaper LightGBM feature additions are exhausted.** | This is where published evidence for LLMs is strongest (local open model 98.23% F1 on messy multilingual record linkage, beating a rule baseline at 91.33%). But it competes against adding cross-source-consensus features to the existing LightGBM selector, which is cheaper, in-stack, and reproducible. LLM only "wins" on the residual hard cases the selector cannot arbitrate. | Strong for LLM-vs-rule; the LLM-vs-*your-tuned-ML* delta is unquantified for this domain |
| **(c) Candidate generation / blocking** | **NO (for LLM).** | Running an LLM over 400k rows × many candidates is the worst cost/latency fit, and direct-LLM geolocation hallucinates. Classical blocking + embedding retrieval dominates and is surveyed separately. | Strong against direct-LLM geocoding |

**Ranking of where an LLM plausibly pays:** (b) tie-breaking > (a) offline dictionary expansion > (c) blocking (no).

**The single most important decision for the human (see §1):** *Does "free/open only in production"
forbid using a paid or any LLM in an **offline** preprocessing step whose committed **output** (an
expanded dictionary, or a set of normalized strings) is thereafter consumed deterministically by the
free/open, reproducible pipeline?* If that offline pattern is acceptable, use cases (a) and offline-(b)
open up cleanly. If the constraint is read strictly (no LLM ever touches any committed artifact), you
are restricted to local open-weight models, run offline, with committed outputs. This is a policy call,
not a technical one, and this document does not decide it.

**Overall confidence:** Medium. The literature strongly supports LLM competence on entity/record
matching *in general* and strongly warns against direct-LLM geocoding. It does **not** contain a study
of Brazilian-Portuguese polling-station matching, nor a head-to-head of an LLM against a
well-tuned domain LightGBM selector — so the marginal value *in this specific pipeline* is
extrapolation, not measurement.

---

## 1. The "free/open in production" decision framing

The constraint given: *free/open tools only in the production pipeline; paid services allowed only in
offline evaluation comparisons; full reproducibility (renv-pinned, deterministic rebuild).* There are
two defensible readings, and they lead to different answers. **This section lays them out; it does not
choose.**

**Reading A — strict ("nothing paid or non-reproducible ever touches a committed artifact").**
A paid batch-API call (e.g. OpenAI Batch, Anthropic) embedded at inference time is out on two grounds
at once: it is not free/open, and it is not reproducible (see §3 — served models change silently). Even
a *local* LLM invoked at `tar_make()` time is suspect under strict reproducibility unless its
determinism is fully pinned (§3). Under this reading, LLMs are confined to `backup/`-style offline
experiments that never feed the committed pipeline.

**Reading B — pragmatic ("the *production pipeline* must be free/open + reproducible; how an input
artifact was *authored* is out of scope").**
Under this reading, an LLM (paid or open) used in a **one-time offline preprocessing step** whose
**output is committed to the repo** — e.g. an expanded abbreviation dictionary, or a table of
normalized strings keyed by raw input — is arguably fine. The production pipeline that rebuilds from
that committed artifact stays free/open and deterministic; the artifact is data, no different from a
hand-curated gazetteer that a human happened to write with tool assistance. The LLM is a *build-time
authoring aid*, not a *runtime dependency*. The reproducibility guarantee attaches to the pipeline, not
to the provenance of every static input.

**Why this is the pivotal decision.** Almost the entire upside of LLMs for this project (use case (a),
and the safest form of (b)) lives in the offline-authoring pattern. If Reading B is acceptable, you can
use the *best available* model (even a paid one) offline in evaluation, commit the vetted output, and
keep production pure. If only Reading A is acceptable, you are limited to local open-weight models and
must additionally defend their determinism. The rest of this document is written so it is useful under
either reading, but flags where the answer depends on this choice.

A concrete guardrail that makes Reading B safer regardless: any LLM-authored artifact should be
(i) fully committed and human-reviewed, (ii) diffable, and (iii) never regenerated as part of the
normal build — regeneration is a manual, logged step. That keeps the pipeline's determinism a property
of committed files, not of a model call.

---

## 2. Open-weight model options (for local, offline, or Reading-A production use)

VRAM figures are rough, for 4-bit quantization (the practical local-inference regime); treat as
order-of-magnitude. "PT suitability" is about Brazilian-Portuguese address/school-name text, which is
mostly a *normalization/matching* task, not deep generation — so even mid-size general models are
plausibly adequate.

| Model | License (and is it OSI-open?) | Rough VRAM (4-bit) | PT-BR suitability | Notes |
|---|---|---|---|---|
| **Qwen2.5 / Qwen3** (0.5–72B) | Most sizes **Apache 2.0 (OSI-open)**; **3B and 72B are under Qwen-specific licenses** (research / special-arrangement commercial) — verify per size | 7B ≈ 5–6 GB; 14B ≈ 9–10 GB; 32B ≈ 20 GB | Strong multilingual; 14B/32B used in address-parsing and record-linkage papers below | Pick a 7B/14B/32B size to stay Apache 2.0; **avoid 3B/72B if you need clean OSI licensing.** [[Qwen2.5 blog]](https://qwenlm.github.io/blog/qwen2.5/) [[Qwen2.5-72B LICENSE]](https://huggingface.co/Qwen/Qwen2.5-72B-Instruct/blob/main/LICENSE) |
| **Llama 3.x** (8B, 70B) | **Llama Community License** — *not* OSI-open. Free under 700M MAU; derivatives must carry "Llama" in the name; acceptable-use policy applies | 8B ≈ 5–6 GB; 70B ≈ 40 GB | Strong; basis for several PT fine-tunes | Custom license, not OSI. Fine for a research project but not "open source" in the strict sense. [[Meta-Llama-3-8B card]](https://huggingface.co/meta-llama/Meta-Llama-3-8B) |
| **Gemma 2 / 3** | **Gemma Terms of Use** — *not* OSI-approved. Commercial allowed but Google may revoke/remotely restrict; prohibits training competing foundation models | 9B ≈ 6–7 GB; 27B ≈ 16–18 GB | Good multilingual | Revocable, non-OSI custom license — weakest fit for a "free/open + reproducible-forever" constraint. [[license discussion]](https://qubittool.com/blog/open-source-ai-license-compliance-guide) (verify against Google's official Gemma terms) |
| **Gervásio 7B (PT)** | **Open, permissive license, commercial OK** (PORTULAN); built on Llama-3 8B | ≈ 5–6 GB | **Purpose-built for Portuguese** (decoder) | Note: inherits Llama-3 lineage; confirm whether upstream Llama terms flow through. [[Gervásio paper]](https://arxiv.org/html/2402.18766v2) [[PORTULAN]](https://huggingface.co/PORTULAN) |
| **Albertina PT (100M/900M/1.5B)** | **Most-permissive open license** (PORTULAN) | <1–2 GB | **PT-BR and PT-PT encoders** — BERT/DeBERTa family | **Encoder, not generative** — cannot "expand abbreviations" by prompting, but is well-suited to *embedding-based blocking/retrieval and similarity features* (use case (c) alternative). [[Albertina paper]](https://arxiv.org/pdf/2403.01897) [[card]](https://huggingface.co/PORTULAN/albertina-900m-portuguese-ptbr-encoder) |
| **Bode 7B/13B (PT)** | **Llama 2 Community License** — not OSI | 7B ≈ 5 GB | PT-BR instruction-tuned (Alpaca-pt) LoRA on Llama 2 | Older base (Llama 2); superseded by Gervásio for quality. [[Bode paper]](https://arxiv.org/html/2401.02909v1) |
| **Sabiá-7B (PT)** | **Research-only** (inherits LLaMA-1 license) — **not usable even in permissive commercial senses** | ≈ 5 GB | PT-BR, but small/old | Weights on HF but license restricts to research. [[card]](https://huggingface.co/maritaca-ai/sabia-7b) |
| **Sabiá-2 / Sabiá-3 / Sabiá-4 (Maritaca)** | **API-only — no open weights distributed** | n/a | Best PT-BR quality of the Sabiá line | **Paid hosted API; not reproducible; production-disqualified.** Usable only in offline eval under Reading A/B. [[Sabiá-2 paper]](https://arxiv.org/html/2403.09887v2) [[Maritaca]](https://www.maritaca.ai/en/) |

**Throughput plausibility for ~400k rows.** A 7–14B model 4-bit on a single modern GPU does on the
order of tens of hundreds of short generations per second with batching; 400k short prompts is a
matter of hours, not days — *feasible for a one-time offline pass*, marginal as a per-`tar_make()`
runtime step. This is engineering-plausible, not benchmarked here. Crucially, the pipeline already
requires 50GB+ RAM and long runs, so a one-time offline LLM pass on a subset (only the ~hard cases)
is not the bottleneck; running it on *every* row *every* rebuild would be.

**Takeaway on models:** For clean OSI licensing + Portuguese, the cleanest combinations are
**Qwen2.5-7B/14B/32B (Apache 2.0)** for generative normalization/matching and **Albertina (permissive)**
for embedding-based retrieval. Gervásio is the strongest *dedicated* PT decoder but carries Llama
lineage worth verifying. Sabiá-3/4 and any hosted API are offline-eval-only.

---

## 3. Reproducibility caveats (these are real, and cited)

The renv-pinned, deterministic-rebuild requirement is the hardest constraint for *any* runtime LLM
step. Bitwise-reproducible LLM output **is achievable, but only under narrow, explicitly-configured
conditions**, and never for hosted APIs.

**What is required for reproducibility, and what still breaks it:**

- **Greedy/temperature-0 is necessary but NOT sufficient.** Temperature 0 controls the *sampling
  logic* but not the *numerical* path; identical prompts can still diverge because dynamic batch size
  changes the internal reduction order of GPU kernels. [[Thinking Machines: Defeating Nondeterminism]](https://thinkingmachines.ai/blog/defeating-nondeterminism-in-llm-inference/)

- **vLLM does not guarantee reproducibility by default.** Official docs: reproducibility holds only
  *"on the same hardware and the same vLLM version."* Determinism requires either
  `VLLM_ENABLE_V1_MULTIPROCESSING=0` (deterministic scheduling, offline only) or the **batch-invariance**
  feature (`VLLM_BATCH_INVARIANT=1`), which needs NVIDIA compute capability ≥8.0. [[vLLM reproducibility docs]](https://docs.vllm.ai/en/latest/usage/reproducibility/) [[vLLM batch invariance]](https://docs.vllm.ai/en/latest/features/batch_invariance/)

- **Batch invariance works but costs throughput.** Batch-invariant kernels give *bit-identical outputs
  across 1,000 repeated runs on Qwen3-8B*, at **~61.5% throughput cost** in the reference
  implementation. So determinism is buyable, at roughly a 1.6× slowdown. [[Thinking Machines]](https://thinkingmachines.ai/blog/defeating-nondeterminism-in-llm-inference/)

- **PyTorch itself does not guarantee cross-environment reproducibility.** Official docs:
  *"Completely reproducible results are not guaranteed across PyTorch releases, individual commits, or
  different platforms,"* and results can differ *between CPU and GPU even with identical seeds.*
  cuDNN autotuning (`torch.backends.cudnn.benchmark`) can pick different algorithms on subsequent runs
  on the same machine. Reproducibility requires seeding torch/NumPy/Python RNGs, disabling autotune, and
  `torch.use_deterministic_algorithms()`. [[PyTorch randomness notes]](https://docs.pytorch.org/docs/stable/notes/randomness.html)

- **llama.cpp: historically non-deterministic on CUDA (GEMM kernels), now has an opt-in deterministic
  mode.** A recent PR adds batch-invariant, fixed-reduction RMSNorm/MatMul/Attention + stable KV-cache,
  giving *bit-identical CUDA inference independent of batch size / concurrency*; CPU single-thread was
  already close to deterministic. So llama.cpp is a viable *local, pinnable* engine, but only with the
  deterministic mode enabled and pinned. [[llama.cpp CUDA non-determinism issue #2838]](https://github.com/ggml-org/llama.cpp/issues/2838) [[llama.cpp deterministic mode PR #16016]](https://github.com/ggml-org/llama.cpp/pull/16016)

**What a reproducible local-LLM step would have to pin (all of these):** weight revision hash, quant
format, inference engine + version, greedy decoding, fixed seed, deterministic-mode / batch-invariance
flag, and the GPU/driver class (or accept CPU-only for portability). That is a long chain to add to an
renv-pinned R pipeline — feasible, but a real maintenance surface.

**Hosted APIs are categorically worse for reproducibility.** Providers update the served model behind a
name silently; even at temperature 0 outputs are not guaranteed identical over time. This is the second,
independent reason (beyond "not free/open") that a paid batch API cannot sit *inside* the reproducible
production pipeline. [[keywordsai: LLM consistency 2025]](https://www.keywordsai.co/blog/llm_consistency_2025)

**Implication for this project.** The reproducibility ledger strongly favors the **offline-authoring
pattern** (Reading B): run the LLM once, commit the vetted output, and let the deterministic pipeline
consume a static file. That sidesteps the entire batch-invariance / kernel-determinism problem, because
the committed artifact is just data. A *runtime* LLM step is possible but pays a determinism tax
(pinned engine + deterministic mode + ~1.6× slowdown) for benefit that §4 argues is marginal.

---

## 4. Per-use-case analysis vs. the cheaper non-LLM alternative

The rule throughout: **an LLM only "wins" where the simpler, in-stack fix doesn't.**

### (a) Normalization / abbreviation expansion

- **What exists today:** deterministic transliteration (Latin-ASCII) + regex + a hand-maintained
  ~60-entry school-synonym abbreviation list in `normalize_school()` (EMEF, EEEF, CMEI, EMEI, creche,
  colégio estadual, …).
- **Cheaper non-LLM alternative:** just extend the hand list / adopt a curated gazetteer or abbreviation
  table. Brazilian school-type abbreviations are a *finite, enumerable* vocabulary — this is close to a
  closed-class problem. Adding entries is cheap, diffable, deterministic, and already the established
  pattern.
- **Where an LLM could add value:** *discovering* abbreviations/variants the hand list is missing — i.e.
  mining the actual raw strings for unseen patterns and proposing dictionary entries. That is a
  **one-time offline dictionary-expansion task**, not per-row inference. The human reviews the proposed
  entries, catching any hallucinated/wrong expansions before they enter the committed list.
- **Evidence:** LLMs are competent at address component extraction/standardization (papers tested
  Qwen2.5-72B, Llama 3, Mistral Large, DeepSeek-R1 on Spanish delivery addresses)
  [[Address Parsing in the Era of LLMs]](https://link.springer.com/chapter/10.1007/978-3-032-10126-6_9),
  **but** the geocoding-parsing benchmark shows ChatGPT *fabricates plausible addresses/coordinates when
  uncertain* and specialized parsers (libpostal) remain superior on structured parsing
  [[Is ChatGPT a game changer for geocoding]](https://arxiv.org/pdf/2310.14360). The fabrication risk is
  exactly why the LLM's role should be *propose-for-human-review dictionary entries*, not silent per-row
  rewriting.
- **Verdict:** **Conditional GO for one-time offline dictionary expansion (human-reviewed, committed);
  NO for per-row production LLM normalization.** The per-row path adds hallucination + determinism risk
  for gains over "extend the list" that are unproven for this closed-class vocabulary. Evidence:
  moderate.

### (b) Hard-case tie-breaking / re-ranking ambiguous candidates

- **What exists today:** Jaro-Winkler / Levenshtein fuzzy features arbitrated by a LightGBM
  distance-regression selector.
- **Cheaper non-LLM alternative:** add features to the LightGBM selector — most obviously
  **cross-source consensus** (does INEP, schools-CNEFE, street-bairro-CNEFE, geocodebr agree?),
  candidate-agreement counts, and disagreement flags. This is in-stack, reproducible, cheap to train,
  and directly targets "which candidate is right." The right first move is almost certainly to exhaust
  these features before reaching for an LLM.
- **Where an LLM could add value:** on the *residual* hard cases where sources disagree and string
  features are ambiguous, an LLM can bring world knowledge (e.g. "EMEF João XXIII" vs "Escola Municipal
  Papa João 23" are the same school; a saint-name vs numeral vs abbreviation equivalence). This is a
  *semantic* judgment classical features approximate poorly.
- **Evidence (this is the strongest LLM case):**
  - **OpenSanctions Pairs** — 755,540 labeled pairs, 293 sources, 31 countries, multilingual/cross-script
    names, noisy/missing attributes (a close analogue to messy Brazilian address/name matching). A
    **production rule-based matcher scored 91.33% F1; off-the-shelf LLMs reached up to 98.95% (GPT-4o) and
    a locally-deployable open model, DeepSeek-R1-Distill-Qwen-14B, reached 98.23%.** The rule system
    *over-matched* (false positives); LLMs failed mainly on cross-script transliteration and minor
    date/identifier mismatches. [[OpenSanctions Pairs]](https://arxiv.org/abs/2603.11051)
  - **Peeters, Steiner & Bizer (2025), Entity Matching using LLMs** — GPT-4 beat fine-tuned Ditto/RoBERTa
    on 3 of 6 datasets; critically, when fine-tuned models faced **unseen entities their F1 collapsed
    22–61%**, while GPT-4 stayed ≥8% above the best transferred model — LLMs are *more robust to
    out-of-distribution* entities. Costs: 23×–102× a cheap baseline; prompt sensitivity varies by model
    (GPT-4 low, some open models high); "no single best prompt." [[Entity Matching using LLMs]](https://arxiv.org/html/2310.11244v3)
  - Earlier Peeters & Bizer (2023): ChatGPT *worse* than a fine-tuned RoBERTa on in-distribution data but
    needs no fine-tuning and generalizes better. [[Using ChatGPT for Entity Matching]](https://arxiv.org/pdf/2305.03423)
  - Baseline context: **Ditto** (fine-tuned PLM EM) already gave +9.43% avg F1 (up to +32%) over
    pre-2020 SOTA — i.e. a *well-tuned ML matcher is itself very strong*, which is what your LightGBM
    selector is closer to. [[Ditto]](https://arxiv.org/abs/2004.00584)
- **The honest gap:** every cited win is **LLM vs. a *rule-based* or *transferred/zero-shot* baseline**.
  None is **LLM vs. a purpose-tuned domain matcher with cross-source consensus features** — which is the
  actual alternative here. The Ditto result is a reminder that a good supervised matcher captures most of
  the gap cheaply. So the *demonstrated* delta is "LLM >> rules"; the *relevant* delta ("LLM > our tuned
  LightGBM with better features") is **plausible but unmeasured** for this domain.
- **Verdict:** **Conditional GO — as an offline evaluation, and only after LightGBM feature additions
  (cross-source consensus) are tried first.** Rank #1 for potential LLM value. If pursued in production
  under Reading B, prefer the offline pattern: run the LLM on the hard-case shortlist, commit its
  verdicts (or use them to train/features the selector), rather than a live LLM call per rebuild.
  Evidence: strong that LLMs beat rules on messy multilingual matching; weak/absent on beating a tuned
  domain model.

### (c) Candidate generation / blocking

- **What exists today / cheaper alternative:** classical blocking (by municipality etc.) and embedding
  retrieval. Portuguese encoders like **Albertina** give cheap, reproducible, GPU-optional dense
  retrieval; this is exactly the classical/embedding lane being surveyed separately.
- **Where an LLM could add value:** essentially nowhere that pays. Generative LLM candidate generation
  over 400k rows × many candidates is the worst cost/latency profile, and asking an LLM to *produce*
  locations directly triggers the hallucination failure mode.
- **Evidence against:** the geospatial-hallucination benchmark (GeoHaluBench, 20 LLMs) shows *uniformly
  low* factual geospatial accuracy — best model **Gemini-2.0-flash ~50.3%**, worst **Mistral-Small-24B
  ~26.9%** on Beijing — with *omission > fabrication* (models lack the knowledge) and a systematic
  **geographic bias against underrepresented regions** (Cairo worse than Beijing). Brazil is plausibly an
  underrepresented region, so direct-LLM location knowledge is exactly where it is weakest.
  [[Geospatial hallucination benchmark]](https://arxiv.org/abs/2507.19586) The geocoding-parsing benchmark
  reaches the same conclusion: LLMs are *not ready to replace* classical geocoding.
  [[Is ChatGPT a game changer for geocoding]](https://arxiv.org/pdf/2310.14360)
- **Verdict:** **NO for LLM.** Use classical blocking + embedding retrieval (Albertina or similar).
  Evidence: strong.

---

## 5. Literature evidence — summary of what is actually demonstrated

| Finding | Source | What it supports / warns |
|---|---|---|
| Fine-tuned PLM matcher (Ditto) +9.43% avg F1 (up to +32%) over prior SOTA; 96.5% F1 on real 789k×412k company match | Li et al. 2020 [[arXiv 2004.00584]](https://arxiv.org/abs/2004.00584) | A **well-tuned supervised matcher is already very strong** — sets the bar the LLM must beat, and it's close to what LightGBM does here |
| ChatGPT worse than fine-tuned RoBERTa in-distribution, but no fine-tuning + better generalization | Peeters & Bizer 2023 [[arXiv 2305.03423]](https://arxiv.org/pdf/2305.03423) | LLM value is **robustness/zero-shot**, not peak in-distribution accuracy |
| GPT-4 beats Ditto/RoBERTa on 3/6 datasets; fine-tuned models drop 22–61% F1 on **unseen** entities while GPT-4 stays ≥8% higher; cost 23×–102×; prompt-sensitivity varies | Peeters, Steiner & Bizer 2025 [[arXiv 2310.11244]](https://arxiv.org/html/2310.11244v3) | Strong support for LLM **tie-breaking on out-of-distribution/hard cases**; flags cost + prompt fragility |
| Rule matcher 91.33% F1 → LLMs up to 98.95% (GPT-4o), **local open model 98.23%** (DeepSeek-R1-Distill-Qwen-14B), on 755k multilingual cross-script pairs; rules over-match, LLMs miss on transliteration | OpenSanctions Pairs [[arXiv 2603.11051]](https://arxiv.org/abs/2603.11051) | **Best evidence for use case (b)** — messy multilingual record linkage, and a *locally deployable open model* nearly matches GPT-4o |
| LLMs competent at address component extraction (Qwen2.5-72B, Llama 3, Mistral Large, DeepSeek-R1 on Spanish addresses) | Address Parsing in the Era of LLMs [[Springer]](https://link.springer.com/chapter/10.1007/978-3-032-10126-6_9) | Supports use case (a) *parsing/normalization competence* |
| ChatGPT **fabricates plausible addresses/coordinates when uncertain**; libpostal/specialized parsers superior; LLMs not ready to replace classical geocoding | "Is ChatGPT a game changer for geocoding" [[arXiv 2310.14360]](https://arxiv.org/pdf/2310.14360) | **Warns against (c)** and against silent per-row LLM normalization in (a) |
| 20 LLMs uniformly low on factual geospatial knowledge (best ~50%, worst ~27%); omission>fabrication (lack knowledge); geographic bias vs underrepresented regions | GeoHaluBench [[arXiv 2507.19586]](https://arxiv.org/abs/2507.19586) | **Strong warning** against trusting LLMs for direct location/coordinate knowledge (relevant to Brazil) |

**Distilled:**
- **(a) normalization** — real published support for *parsing competence*, real published warning about
  *fabrication*. Net: use LLM to *propose* dictionary entries offline (human-reviewed), not to rewrite
  rows. **Support: moderate.**
- **(b) tie-breaking** — strongest published support (OpenSanctions, Peeters). But all wins are vs.
  *rules/zero-shot*, not vs. a tuned domain model with consensus features. **Support: strong vs rules;
  speculative vs your tuned selector.**
- **(c) blocking / direct geocoding** — published evidence is *against* LLMs (hallucination, geographic
  bias). **Support for NOT using LLM: strong.**

---

## 6. Recommended path + open decision for the human

**Recommended path (engineering):**
1. **Do the cheap non-LLM things first.** Extend the school-synonym list / adopt a curated abbreviation
   gazetteer (use case a), and add **cross-source consensus features** to the LightGBM selector
   (use case b). These are in-stack, reproducible, and likely capture most of the addressable error.
   The Ditto result is the reminder that a well-tuned supervised matcher is hard to beat.
2. **Use an LLM only offline, only where step 1 leaves residual error.** The highest-value, lowest-risk
   LLM application is a **one-time, human-reviewed dictionary/normalization-proposal pass** (a) and/or an
   **offline hard-case adjudication / label-generation pass** to improve the selector (b). Commit the
   vetted outputs; keep them out of the runtime build path.
3. **If you evaluate LLMs, do it as an offline comparison** (allowed for paid models under the stated
   constraint). Benchmark a local open model (Qwen2.5-14B Apache-2.0, or Gervásio for PT) against a
   paid frontier model on a labeled sample of *hard* polling-station cases, measuring the delta over the
   improved LightGBM selector — not over the current baseline. That is the missing measurement.
4. **Do NOT put an LLM in candidate generation / blocking, and never ask an LLM for coordinates
   directly.** Use classical blocking + Portuguese-encoder embedding retrieval (Albertina) instead.
5. **If any LLM step ever enters production, use llama.cpp or vLLM in deterministic/batch-invariant mode,
   pin weight-hash + engine-version + seed + greedy decoding,** and accept the ~1.6× throughput cost —
   but prefer the offline-committed-artifact pattern that avoids this entirely.

**Open decision the human must make (the pivotal one):**
Adopt **Reading A or Reading B** of "free/open in production" (§1). Specifically: *is a one-time offline
LLM step, whose human-reviewed output is committed and consumed deterministically, acceptable inside a
"free/open + reproducible" project?* If **yes (Reading B)**, the recommended path above is fully
available, including using the best (even paid) model offline. If **no (Reading A)**, restrict to local
open-weight models (Qwen2.5-Apache / Gervásio / Albertina) and defend their determinism per §3 — the
upside shrinks but does not vanish. This is a policy judgment about the spirit of the constraint, and it
should be made explicitly before any LLM work is scheduled.

**Bottom line:** No use case justifies an LLM as a *runtime* production dependency today. Use case (b)
tie-breaking has the strongest evidence and is worth an *offline* evaluation, but only after cheaper
LightGBM features are exhausted; use case (a) is worth an *offline, human-reviewed* dictionary-expansion
pass; use case (c) is a clear no for LLMs.

---

## Sources

**Inference-engine / framework reproducibility (primary docs):**
- vLLM — Reproducibility: https://docs.vllm.ai/en/latest/usage/reproducibility/
- vLLM — Batch Invariance: https://docs.vllm.ai/en/latest/features/batch_invariance/
- PyTorch — Reproducibility notes: https://docs.pytorch.org/docs/stable/notes/randomness.html
- llama.cpp — CUDA non-determinism issue #2838: https://github.com/ggml-org/llama.cpp/issues/2838
- llama.cpp — Deterministic inference mode PR #16016: https://github.com/ggml-org/llama.cpp/pull/16016
- Thinking Machines Lab — Defeating Nondeterminism in LLM Inference: https://thinkingmachines.ai/blog/defeating-nondeterminism-in-llm-inference/
- keywordsai — LLM consistency 2025 (hosted-API non-reproducibility): https://www.keywordsai.co/blog/llm_consistency_2025

**Model cards / licenses (primary):**
- Qwen2.5 blog (license by size): https://qwenlm.github.io/blog/qwen2.5/
- Qwen2.5-72B LICENSE: https://huggingface.co/Qwen/Qwen2.5-72B-Instruct/blob/main/LICENSE
- Meta-Llama-3-8B card: https://huggingface.co/meta-llama/Meta-Llama-3-8B
- Gervásio PT (paper): https://arxiv.org/html/2402.18766v2 ; PORTULAN models: https://huggingface.co/PORTULAN
- Albertina PT* family (paper): https://arxiv.org/pdf/2403.01897 ; card: https://huggingface.co/PORTULAN/albertina-900m-portuguese-ptbr-encoder
- Bode PT (paper): https://arxiv.org/html/2401.02909v1 ; card: https://huggingface.co/recogna-nlp/bode-7b-alpaca-pt-br
- Sabiá-7B card (research-only): https://huggingface.co/maritaca-ai/sabia-7b
- Sabiá-2 (paper): https://arxiv.org/html/2403.09887v2 ; Maritaca (API-only line): https://www.maritaca.ai/en/
- AI-license overview (Gemma/Llama/Apache comparison, secondary): https://qubittool.com/blog/open-source-ai-license-compliance-guide

**Entity matching / record linkage (primary):**
- Li et al. 2020, Ditto — Deep Entity Matching with Pre-Trained LMs: https://arxiv.org/abs/2004.00584
- Peeters & Bizer 2023 — Using ChatGPT for Entity Matching: https://arxiv.org/pdf/2305.03423
- Peeters, Steiner & Bizer 2025 — Entity Matching using LLMs: https://arxiv.org/html/2310.11244v3
- OpenSanctions Pairs — Large-Scale Entity Matching with LLMs: https://arxiv.org/abs/2603.11051

**Address parsing / geocoding / geospatial hallucination (primary):**
- Address Parsing in the Era of LLMs — A Comparative Analysis: https://link.springer.com/chapter/10.1007/978-3-032-10126-6_9
- "Is ChatGPT a game changer for geocoding" (benchmark): https://arxiv.org/pdf/2310.14360
- Mitigating Geospatial Knowledge Hallucination in LLMs (GeoHaluBench, EMNLP-Findings 2025): https://arxiv.org/abs/2507.19586
