#!/bin/bash

# Ralph Auto Loop - Autonomous AI coding agent that implements specs
#
# This script runs an autonomous agent (via OpenCode) to implement a specific task.
# A focus prompt is REQUIRED - the agent will only do what you ask.

set -e
set -o pipefail

SKIP_CHECKS=false
FOCUS_PROMPT=""
MAX_ITERATIONS=0
JUDGE_PROMPT_FILE=""
JUDGE_FIRST=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-checks) SKIP_CHECKS=true; shift ;;
        --max-iterations)
            if [[ -n "$2" && "$2" =~ ^[0-9]+$ ]]; then
                MAX_ITERATIONS="$2"; shift 2
            else
                echo "Error: --max-iterations requires a positive integer"; exit 1
            fi ;;
        --judge)
            if [[ -n "$2" && -f "$2" ]]; then
                JUDGE_PROMPT_FILE="$2"; shift 2
            else
                echo "Error: --judge requires a path to an existing prompt file"; exit 1
            fi ;;
        --judge-first) JUDGE_FIRST=true; shift ;;
        --help|-h)
            echo "Usage: ./ralph-auto.sh <focus prompt> [options]"
            echo "Options: --skip-checks, --max-iterations <n>, --judge <file>, --judge-first, --help"
            echo "Environment: OPENCODE_MODEL (default: anthropic/claude-opus-4-5)"
            echo "Environment: OPENCODE_VARIANT (default: high)"
            exit 0 ;;
        -*) echo "Unknown option: $1"; exit 1 ;;
        *)
            if [[ -z "$FOCUS_PROMPT" ]]; then FOCUS_PROMPT="$1"
            else echo "Error: Multiple focus prompts provided"; exit 1
            fi
            shift ;;
    esac
done

if [[ -z "$FOCUS_PROMPT" ]]; then
    echo "Error: A focus prompt is required"
    echo "Usage: ./ralph-auto.sh <focus prompt> [options]"
    exit 1
fi

PROMPT_TEMPLATE="RALPH_AUTO_PROMPT.md"
COMPLETE_MARKER="NOTHING_LEFT_TO_DO"
OUTPUT_DIR=".ralph-auto"
OPENCODE_MODEL="${OPENCODE_MODEL:-anthropic/claude-opus-4-5}"
OPENCODE_VARIANT="${OPENCODE_VARIANT:-high}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

cleanup() {
    pkill -P $$ 2>/dev/null || true
    if [ -d "$OUTPUT_DIR" ]; then
        rm -rf "$OUTPUT_DIR"
        echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} Cleaned up $OUTPUT_DIR"
    fi
}

handle_signal() {
    echo ""
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} Received interrupt signal, shutting down..."
    cleanup
    exit 130
}

trap cleanup EXIT
trap handle_signal INT TERM

mkdir -p "$OUTPUT_DIR"

log() {
    local level=$1; shift
    local message="$@"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    case $level in
        "INFO")  echo -e "${BLUE}[$timestamp]${NC} $message" ;;
        "SUCCESS") echo -e "${GREEN}[$timestamp]${NC} $message" ;;
        "WARN")  echo -e "${YELLOW}[$timestamp]${NC} $message" ;;
        "ERROR") echo -e "${RED}[$timestamp]${NC} $message" ;;
    esac
    echo "[$timestamp] [$level] $message" >> "$OUTPUT_DIR/ralph-auto.log"
}

check_prerequisites() {
    log "INFO" "Checking prerequisites..."
    command -v opencode &> /dev/null || { log "ERROR" "OpenCode is not installed"; exit 1; }
    git rev-parse --git-dir > /dev/null 2>&1 || { log "ERROR" "Not in a git repository"; exit 1; }
    [ -d "specs" ] || { log "ERROR" "specs/ directory not found"; exit 1; }
    local spec_count=$(find specs -name "*.md" -type f | wc -l | tr -d ' ')
    [ "$spec_count" -eq 0 ] && { log "ERROR" "No .md files in specs/"; exit 1; }
    log "INFO" "Found $spec_count actionable spec(s) in specs/"
    [ -f "$PROMPT_TEMPLATE" ] || { log "ERROR" "$PROMPT_TEMPLATE not found"; exit 1; }
    log "SUCCESS" "Prerequisites check passed"
}

has_changes() {
    ! git diff --quiet || ! git diff --cached --quiet || [ -n "$(git ls-files --others --exclude-standard)" ]
}

# Filter opencode JSON output
stream_filter() {
    local last_was_tool=false
    while IFS= read -r line; do
        local event_type
        event_type=$(echo "$line" | jq -r '.type // empty' 2>/dev/null)
        case "$event_type" in
            "text"|"message"|"assistant"|"output_text"|"output_text_delta"|"final")
                local text
                text=$(echo "$line" | jq -r '
                    if .type == "text" then .part.text // empty
                    elif .type == "output_text" then .part.text // .text // empty
                    elif .type == "output_text_delta" then .part.delta // .delta // empty
                    elif .type == "final" then .part.text // .text // empty
                    elif .type == "message" or .type == "assistant" then
                        (
                            .part.text // empty,
                            (.part.content[]? | select(.type == "text" or .type == "output_text") | .text // empty),
                            (.part.content[]? | select(.type == "output_text_delta") | .delta // empty)
                        )
                    else empty end
                ' 2>/dev/null)
                if [ -n "$text" ]; then
                    $last_was_tool && echo ""
                    printf "%s" "$text"
                    last_was_tool=false
                fi
                ;;
            "tool_use")
                local tool_name tool_detail input
                tool_name=$(echo "$line" | jq -r '.part.tool // empty' 2>/dev/null)
                tool_detail=$(echo "$line" | jq -r '.part.state.title // empty' 2>/dev/null)
                if [ -z "$tool_detail" ]; then
                    input=$(echo "$line" | jq -c '.part.state.input // {}' 2>/dev/null)
                    case "$tool_name" in
                        read)      tool_detail=$(echo "$input" | jq -r '.filePath // empty') ;;
                        write)     tool_detail=$(echo "$input" | jq -r '.filePath // empty') ;;
                        edit)      tool_detail=$(echo "$input" | jq -r '.filePath // empty') ;;
                        glob)      tool_detail=$(echo "$input" | jq -r '.pattern // empty') ;;
                        grep)      tool_detail=$(echo "$input" | jq -r '"\(.pattern // "")" + (if .include then " (\(.include))" else "" end)') ;;
                        bash)      tool_detail=$(echo "$input" | jq -r '.description // (.command | .[0:50]) // empty') ;;
                        task)      tool_detail=$(echo "$input" | jq -r '.description // empty') ;;
                        webfetch)  tool_detail=$(echo "$input" | jq -r '.url // empty' | head -c 50) ;;
                        question)  tool_detail=$(echo "$input" | jq -r '.questions[0].header // empty') ;;
                        todowrite) tool_detail="updating todos" ;;
                        todoread)  tool_detail="reading todos" ;;
                        skill)     tool_detail=$(echo "$input" | jq -r '.name // empty') ;;
                    esac
                fi
                if [ -n "$tool_name" ]; then
                    $last_was_tool || echo ""
                    echo -e "${BLUE}> ${tool_name}${tool_detail:+: $tool_detail}${NC}"
                    last_was_tool=true
                fi
                ;;
            "error")
                local error_msg
                error_msg=$(echo "$line" | jq -r '.error.message // .error // "Unknown error"' 2>/dev/null)
                echo -e "\n${RED}Error: $error_msg${NC}"
                last_was_tool=false
                ;;
            "step_finish")
                $last_was_tool || echo ""
                ;;
        esac
    done
    echo ""
}

run_ci_checks() {
    log "INFO" "Running CI checks..."
    local ci_failed=0
    local error_output=""

    echo "=========================================="
    echo "Running CI Checks"
    echo "=========================================="

    echo -e "\n1. Rust Analyzer Diagnostics...\n-------------------------------"
    local ra_output
    ra_output=$(./build-env run "rust-analyzer diagnostics ." 2>&1 | grep -E "^Error.*RustcHardError" || true)
    if [ -n "$ra_output" ]; then
        echo -e "${RED}Rust Analyzer found errors${NC}"
        ci_failed=1
        error_output+="## Rust Analyzer Errors\n\`\`\`\n$ra_output\n\`\`\`\n\n"
    else
        echo -e "${GREEN}Rust Analyzer passed${NC}"
    fi

    echo -e "\n2. Building (eBPF + Userspace)...\n----------------------------------"
    local build_output
    if build_output=$(./build-env build debug 2>&1); then
        echo -e "${GREEN}Build passed${NC}"
    else
        echo -e "${RED}Build failed${NC}"
        ci_failed=1
        error_output+="## Build Failed\n\`\`\`\n$build_output\n\`\`\`\n\n"
    fi

    echo -e "\n3. Clippy (Linting)...\n----------------------"
    local clippy_output
    if clippy_output=$(./build-env clippy 2>&1); then
        echo -e "${GREEN}Clippy passed${NC}"
    else
        echo -e "${RED}Clippy failed${NC}"
        ci_failed=1
        error_output+="## Clippy Failed\n\`\`\`\n$clippy_output\n\`\`\`\n\n"
    fi

    echo -e "\n4. Clippy eBPF (Linting)...\n---------------------------"
    local clippy_ebpf_output
    if clippy_ebpf_output=$(./build-env clippy-ebpf 2>&1); then
        echo -e "${GREEN}Clippy eBPF passed${NC}"
    else
        echo -e "${RED}Clippy eBPF failed${NC}"
        ci_failed=1
        error_output+="## Clippy eBPF Failed\n\`\`\`\n$clippy_ebpf_output\n\`\`\`\n\n"
    fi

    echo -e "\n5. Documentation Check...\n-------------------------"
    local doc_output
    if doc_output=$(./build-env doc 2>&1); then
        echo -e "${GREEN}Documentation check passed${NC}"
    else
        echo -e "${RED}Documentation check failed${NC}"
        ci_failed=1
        error_output+="## Documentation Check Failed\n\`\`\`\n$doc_output\n\`\`\`\n\n"
    fi

    echo -e "\n6. Running Tests...\n-------------------"
    local test_output
    if test_output=$(./build-env test 2>&1); then
        echo -e "${GREEN}Tests passed${NC}"
    else
        echo -e "${RED}Tests failed${NC}"
        ci_failed=1
        error_output+="## Tests Failed\n\`\`\`\n$test_output\n\`\`\`\n\n"
    fi

    echo -e "\n=========================================="
    if [ $ci_failed -eq 0 ]; then
        echo -e "${GREEN}All CI checks passed!${NC}"
        log "SUCCESS" "CI checks passed"
        return 0
    else
        echo -e "${RED}CI checks failed!${NC}"
        log "ERROR" "CI checks failed"
        echo -e "# CI Check Failures\n\n$error_output" > "$OUTPUT_DIR/ci_errors.txt"
        return 1
    fi
}

commit_changes() {
    local iteration="$1" task_summary="$2"
    log "INFO" "Committing changes..."
    git add -A
    git diff --cached --quiet && { log "WARN" "No changes to commit"; return 0; }
    if git commit -m "feat(auto): $task_summary

Ralph-Auto-Iteration: $iteration

Automated commit by Ralph Auto loop."; then
        log "SUCCESS" "Committed: $task_summary"
    else
        log "ERROR" "Commit failed"; return 1
    fi
}

rollback_changes() {
    log "WARN" "Rolling back uncommitted changes..."
    git checkout -- .
    git clean -fd
}

build_prompt() {
    local iteration=$1 ci_errors="" focus_section=""

    [ -f "$OUTPUT_DIR/ci_errors.txt" ] && ci_errors="## Previous Iteration Errors

**CI checks failed. You MUST fix these errors.**

Read: \`$OUTPUT_DIR/ci_errors.txt\`
"

    [ -n "$FOCUS_PROMPT" ] && focus_section="## FOCUS MODE

**Work ONLY on:** $FOCUS_PROMPT

Signal TASK_COMPLETE when done.
"

    local specs_list=$(find specs -name "*.md" -type f | sort | while read f; do echo "- \`$f\`"; done)
    local prompt=$(cat "$PROMPT_TEMPLATE")
    prompt="${prompt//\{\{SPECS_LIST\}\}/$specs_list}"
    prompt="${prompt//\{\{ITERATION\}\}/$iteration}"
    prompt="${prompt//\{\{CI_ERRORS\}\}/$ci_errors}"
    prompt="${prompt//\{\{FOCUS\}\}/$focus_section}"
    echo "$prompt"
}

extract_task_description() {
    local output_file="$1"
    local desc=$(cat "$output_file" | jq -r '
        if .type == "text" then .part.text // empty
        elif .type == "output_text" then .part.text // .text // empty
        elif .type == "output_text_delta" then .part.delta // .delta // empty
        elif .type == "final" then .part.text // .text // empty
        elif .type == "message" or .type == "assistant" then
            (
                .part.text // empty,
                (.part.content[]? | select(.type == "text" or .type == "output_text") | .text // empty),
                (.part.content[]? | select(.type == "output_text_delta") | .delta // empty)
            )
        else empty end
    ' 2>/dev/null | grep "TASK_COMPLETE:" | head -1 | sed 's/.*TASK_COMPLETE:[[:space:]]*//')
    echo "${desc:-Autonomous improvements}"
}

run_judge() {
    local iteration=$1
    local judge_output_file="$OUTPUT_DIR/iteration_${iteration}_judge_output.txt"
    local judge_stderr_file="$OUTPUT_DIR/iteration_${iteration}_judge_stderr.txt"
    local judge_prompt_file="$OUTPUT_DIR/iteration_${iteration}_judge_prompt.md"

    log "INFO" "Running judging agent..."

    cp "$JUDGE_PROMPT_FILE" "$judge_prompt_file"

    # Prepend verdict signal instructions to the judge prompt (more visible at top)
    local original_prompt
    original_prompt=$(cat "$judge_prompt_file")
    cat > "$judge_prompt_file" <<'SIGNALS'
# CRITICAL: VERDICT REQUIRED

After completing your review, you MUST output exactly one of these verdict signals on its own line:

```
MORE_WORK_TO_DO
```
OR
```
ALL_WORK_DONE
```

**If you find ANY issues, problems, or incomplete work:** Output `MORE_WORK_TO_DO`
**If everything is complete and correct:** Output `ALL_WORK_DONE`

This signal is MANDATORY. Your response will be rejected without it.

---

SIGNALS
    echo "$original_prompt" >> "$judge_prompt_file"
    
    # Also append reminder at the end
    cat >> "$judge_prompt_file" <<'SIGNALS_END'

---

# REMINDER: Output Your Verdict

You MUST end your response with exactly one of:
- `MORE_WORK_TO_DO` - if any issues remain
- `ALL_WORK_DONE` - if everything is complete
SIGNALS_END

    local judge_exit_code=0
    if cat "$judge_prompt_file" | opencode run --model "$OPENCODE_MODEL" --variant "$OPENCODE_VARIANT" --format json 2>"$judge_stderr_file" | tee "$judge_output_file" | stream_filter; then
        log "SUCCESS" "Judge completed"
    else
        judge_exit_code=$?
        if [ $judge_exit_code -eq 130 ] || [ $judge_exit_code -eq 143 ]; then
            log "INFO" "Judge interrupted"; return 2
        fi
        log "WARN" "Judge exited with status $judge_exit_code"
    fi

    local judge_text=$(cat "$judge_output_file" | jq -r '
        if .type == "text" then .part.text // empty
        elif .type == "output_text" then .part.text // .text // empty
        elif .type == "output_text_delta" then .part.delta // .delta // empty
        elif .type == "final" then .part.text // .text // empty
        elif .type == "message" or .type == "assistant" then
            (
                .part.text // empty,
                (.part.content[]? | select(.type == "text" or .type == "output_text") | .text // empty),
                (.part.content[]? | select(.type == "output_text_delta") | .delta // empty)
            )
        else empty end
    ' 2>/dev/null)

    if echo "$judge_text" | grep -qE "(MORE_WORK_TO_DO|MORE WORK TO DO)"; then
        log "WARN" "Judge says: MORE_WORK_TO_DO"
        return 1
    elif echo "$judge_text" | grep -qE "(ALL_WORK_DONE|ALL WORK DONE)"; then
        log "SUCCESS" "Judge says: ALL_WORK_DONE"
        return 0
    else
        log "ERROR" "Judge did not produce a verdict (expected MORE_WORK_TO_DO or ALL_WORK_DONE)"
        log "WARN" "Assuming MORE_WORK_TO_DO to be safe"
        return 1
    fi
}

run_iteration() {
    local iteration=$1
    local output_file="$OUTPUT_DIR/iteration_${iteration}_output.txt"
    local stderr_file="$OUTPUT_DIR/iteration_${iteration}_stderr.txt"
    local prompt_file="$OUTPUT_DIR/iteration_${iteration}_prompt.md"

    log "INFO" "Starting iteration $iteration"

    build_prompt "$iteration" > "$prompt_file"
    log "INFO" "Prompt: $(wc -l < "$prompt_file" | tr -d ' ') lines"

    log "INFO" "Running OpenCode agent..."
    echo ""

    local agent_exit_code=0
    if cat "$prompt_file" | opencode run --model "$OPENCODE_MODEL" --variant "$OPENCODE_VARIANT" --format json 2>"$stderr_file" | tee "$output_file" | stream_filter; then
        log "SUCCESS" "Agent completed iteration $iteration"
    else
        agent_exit_code=$?
        if [ $agent_exit_code -eq 130 ] || [ $agent_exit_code -eq 143 ]; then
            log "INFO" "Agent interrupted"; return 1
        fi
        log "WARN" "Agent exited with status $agent_exit_code"
    fi

    local assistant_text=$(cat "$output_file" | jq -r '
        if .type == "text" then .part.text // empty
        elif .type == "output_text" then .part.text // .text // empty
        elif .type == "output_text_delta" then .part.delta // .delta // empty
        elif .type == "final" then .part.text // .text // empty
        elif .type == "message" or .type == "assistant" then
            (
                .part.text // empty,
                (.part.content[]? | select(.type == "text" or .type == "output_text") | .text // empty),
                (.part.content[]? | select(.type == "output_text_delta") | .delta // empty)
            )
        else empty end
    ' 2>/dev/null)
    local has_task_complete=false has_nothing_left=false
    echo "$assistant_text" | grep -q "TASK_COMPLETE" && has_task_complete=true
    echo "$assistant_text" | grep -q "$COMPLETE_MARKER" && has_nothing_left=true

    if [ "$has_task_complete" = true ]; then
        log "INFO" "Agent signaled task completion"
        local task_desc=$(extract_task_description "$output_file")
        local ci_passed=true
        [ "$SKIP_CHECKS" = true ] && log "INFO" "Skipping CI checks" || { run_ci_checks || ci_passed=false; }
        if [ "$ci_passed" = true ]; then
            rm -f "$OUTPUT_DIR/ci_errors.txt"
            commit_changes "$iteration" "$task_desc" || { rollback_changes; return 1; }
            log "SUCCESS" "Task completed: $task_desc"
        else
            log "WARN" "CI failed - keeping changes for next iteration"
            return 1
        fi
    elif has_changes; then
        log "WARN" "No TASK_COMPLETE but has changes"
        local ci_passed=true
        [ "$SKIP_CHECKS" = true ] || { run_ci_checks || ci_passed=false; }
        [ "$ci_passed" = true ] && { rm -f "$OUTPUT_DIR/ci_errors.txt"; commit_changes "$iteration" "Partial work"; }
    fi

    if [ "$has_nothing_left" = true ]; then
        log "SUCCESS" "Agent signaled NOTHING_LEFT_TO_DO"
        if [ -n "$JUDGE_PROMPT_FILE" ]; then
            local judge_result=0
            run_judge "$iteration" || judge_result=$?
            if [ $judge_result -eq 1 ]; then
                log "INFO" "Resuming main agent loop per judge verdict"
                return 1
            elif [ $judge_result -eq 2 ]; then
                return 1
            fi
        fi
        return 0
    fi
    return 1
}

main() {
    log "INFO" "=========================================="
    log "INFO" "Starting Ralph Auto Loop"
    log "INFO" "=========================================="
    log "INFO" "Focus: $FOCUS_PROMPT"
    [ "$MAX_ITERATIONS" -gt 0 ] && log "INFO" "Max iterations: $MAX_ITERATIONS"
    [ -n "$JUDGE_PROMPT_FILE" ] && log "INFO" "Judge prompt: $JUDGE_PROMPT_FILE"
    [ "$JUDGE_FIRST" = true ] && log "INFO" "Judge-first: enabled"
    [ "$SKIP_CHECKS" = true ] && log "WARN" "Skip checks: enabled"

    check_prerequisites

    local start_time=$(date +%s) iteration=1 completed=false

    if [ "$SKIP_CHECKS" = true ]; then
        log "INFO" "Skipping initial CI checks"
        rm -f "$OUTPUT_DIR/ci_errors.txt"
    else
        log "INFO" "Running initial CI checks..."
        run_ci_checks && rm -f "$OUTPUT_DIR/ci_errors.txt" || log "WARN" "Initial CI failed"
    fi

    if [ "$JUDGE_FIRST" = true ] && [ -n "$JUDGE_PROMPT_FILE" ]; then
        log "INFO" "Running judge before main loop..."
        local judge_result=0
        run_judge 0 || judge_result=$?
        if [ $judge_result -eq 0 ]; then
            log "SUCCESS" "Judge says ALL_WORK_DONE before starting"
            completed=true
        elif [ $judge_result -eq 2 ]; then
            log "INFO" "Judge interrupted"
            exit 130
        else
            log "INFO" "Judge found issues, proceeding to main loop"
        fi
    fi

    if [ "$completed" = true ]; then
        log "INFO" "=========================================="
        log "INFO" "Complete. Iterations: 0, Duration: $(($(date +%s) - start_time))s"
        log "SUCCESS" "All work completed (judge-first)!"
        exit 0
    fi

    while true; do
        log "INFO" "------------------------------------------"
        log "INFO" "ITERATION $iteration"
        log "INFO" "------------------------------------------"

        if run_iteration $iteration; then
            log "SUCCESS" "Nothing left to do!"
            completed=true
            break
        fi

        [ "$MAX_ITERATIONS" -gt 0 ] && [ "$iteration" -ge "$MAX_ITERATIONS" ] && {
            log "WARN" "Reached max iterations"; break
        }

        sleep 2
        ((iteration++))
    done

    log "INFO" "=========================================="
    log "INFO" "Complete. Iterations: $iteration, Duration: $(($(date +%s) - start_time))s"
    [ "$completed" = true ] && log "SUCCESS" "All work completed!"
    log "INFO" "Recent commits:"
    git log --oneline -5 --grep="Ralph-Auto" || true
    exit 0
}

main
