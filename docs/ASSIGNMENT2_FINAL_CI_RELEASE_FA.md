# Final CI and Docker Hub release gates - Assignment 2

## Target

- Working branch: `feature/bonus10e-source-foundation`
- Last known passing source/archive commit: `84f4cf7` (short SHA, November 8, 2026).
- Original `main` and running VPS remain unchanged until an explicit deployment decision.
- Core Compose still uses A/B; the integrated B/C bonus implementation lives in a separate versioned lab under `labs/`.

## Evidence levels

1. **Offline/static (performed by source review):** 290 Python tests PASS on a reconstructed copy of the source plus the integrated lab; workflow and release checks are independently validated.
2. **CI pending:** GitHub Actions must rerun 290 tests from the exact remote commit, compile sources, validate Compose definitions, and build the application image. Do not claim this has passed before GitHub shows green checks.
3. **Image publish pending:** After CI on the feature branch, merge the reviewed Pull Request to the default branch `main` and confirm CI also passes on the merge commit. GitHub exposes `workflow_dispatch` workflows in the Actions UI after the workflow exists on the default branch; run `Assignment 2 - Manual Docker Hub Publish` from `main`. Configure `DOCKERHUB_USERNAME` repository Actions variable plus `DOCKERHUB_TOKEN` secret. Do not share the token in ChatGPT, files, or logs.
4. **Published digest pending:** The workflow prints a digest in GitHub job summary. Copy that digest into release records. The existing `compose.release.yaml` is pinned to the **previous phase8a** image and must not be described as the new image until independently updated and verified.

## GitHub UI procedure (no automatic deployment)

1. Install the package via Windows PowerShell. It verifies local/remote exact starting commit, adds new CI files only, commits, then pushes the feature branch.
2. Open **Actions -> Assignment 2 - Final CI Gates** and wait for both `Python regression and Compose validation` and `Build core application image` to pass. Check exact SHA.
3. Open a Pull Request to `main`, review changes and pass checks. Do not merge a failed workflow.
4. To publish a core app image: configure the Docker Hub Actions repository variable and secret; **after merging and checking the CI of `main`**, run the manual publish workflow from **main**. It will publish `a2-final-<12 SHA characters of the main commit>` and show the digest.
5. Do not change the live VPS, database volumes, or Assignment 1 during this process.

## Academic scope and limitations

- Main assignment: two shards A/B, two replicas each, 2PC and seven experiments PASS in previous VPS evidence.
- Self-Healing: replica repair worked; an independent controller-crash/no-duplicate-replica proof still requires completion.
- Three-shard bonus: isolated lab completed controlled B-to-C cutover with 500 accounts, epoch 2, 2PC afterwards and Crash Matrix recovery; full live zero-downtime is **not** proven.
- Availability evidence during Map publication: approximate max failed-read streak 51.5 seconds, max failed-readiness streak 68.4 seconds. The measure does not claim how many financial requests failed.
- The instructor's full bonus criteria (live migration with continued availability, request IDs/exactly-once, real container crashes at specific points including after map activation/before cleanup) are not completely demonstrated by these tests.
- The Dockerfile builds a self-contained **application-core** image; bonus migration requires lab-specific SQL, Compose and orchestration files and is not a one-command production deployment.
- Report <=10 A4 pages, demo video <=15 minutes, and submission to LMS/GitHub/Docker Hub as prescribed by the assignment PDF.
