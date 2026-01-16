# CLAUDE.md - AI Assistant Guidelines for GameFlowData

This file provides guidance for AI assistants working on this project.

<!-- SOLOKIT_GUIDANCE -->

---

## Solokit Session Management

This project uses Solokit for Session-Driven Development. Follow these guidelines for consistent and effective usage.

**Quality Tier**: Tier 1 Essential
**Test Coverage Target**: 60%
**Adopted With**: Solokit v0.3.0

### Understanding Solokit Commands

Solokit commands are available as **slash commands** in Claude Code (e.g., `/start`, `/end`, `/work-new`) or via the `sk` CLI in terminal. **Slash commands are preferred** as they provide interactive prompts.

For CLI usage with specific arguments, use `--help` to discover options:

```bash
sk <command> --help
```

### Work Item Management

#### Creating Work Items

**When asked to create a work item, ALWAYS use the CLI:**

```bash
# Check available options first
sk work-new --help

# Create with required fields
sk work-new --type feature --title "Add user authentication" --priority high

# With dependencies
sk work-new --type feature --title "Add OAuth" --priority high --dependencies feat_user_auth

# Mark as urgent
sk work-new --type bug --title "Fix critical login error" --priority critical --urgent
```

**NEVER create work items by directly editing `work_items.json`.**

**Valid Types**: feature, bug, refactor, security, integration_test, deployment
**Valid Priorities**: critical, high, medium, low

#### Listing and Viewing Work Items

```bash
# List all work items
sk work-list

# Filter by status
sk work-list --status not_started
sk work-list --status in_progress

# Filter by type
sk work-list --type bug

# View details
sk work-show <work_item_id>
```

#### Updating Work Items

```bash
# Update status
sk work-update feat_001 --status in_progress

# Update priority
sk work-update feat_001 --priority critical

# Add dependency
sk work-update feat_001 --add-dependency feat_002

# Mark as urgent
sk work-update feat_001 --set-urgent
```

### Spec File Guidelines

#### Spec File Location
- Spec files are stored in `.session/specs/`
- Each work item gets a spec file: `.session/specs/{work_item_id}.md`

#### Spec File Best Practices

1. **Always use the template structure** - Don't create spec files from scratch
2. **Be thorough and consistent** - Give equal attention to each spec file
3. **Include acceptance criteria** - Every spec must have clear, testable criteria
4. **Link related work items** - Reference dependencies in the spec

### Session Workflow

#### Starting a Session

Use `/start` to begin a session:

```bash
/start                    # Interactive - select from available work items
/start <work_item_id>     # Start specific work item
```

The start command:
- Updates work item status to `in_progress`
- Generates a session briefing with full context
- Provides information about dependencies and related learnings

#### Checking Status

Use `/status` to check current session:

```bash
/status
```

Shows current work item, session duration, and quality gate status.

#### Validating Quality

Use `/validate` to check quality gates:

```bash
/validate
```

Runs quality gates without ending the session. Use frequently during development.

#### Ending a Session

Use `/end` to complete a session:

```bash
/end
```

The end command:
- Runs all quality gate validations
- Prompts for session summary
- Updates work item status if complete
- Prompts for learning capture

**IMPORTANT**: Always end sessions properly. Don't abandon sessions.

### Learning Capture

#### When to Capture Learnings

Capture learnings when you:
- Solve a tricky problem
- Discover a better pattern
- Find an important gotcha
- Learn something about the codebase

#### How to Capture Learnings

**Method 1: During Session End (Preferred)**
When running `/end`, you'll be prompted to capture learnings.

**Method 2: Explicit Command**
```bash
/learn
```

#### Searching and Viewing Learnings

```bash
/learn-search "authentication"          # Search learnings
/learn-show                             # Show all learnings
/learn-show --category debugging        # Filter by category
```

### Dependency Graph

```bash
/work-graph                       # Generate dependency graph
/work-graph --focus feat_001      # Focus on specific work item
/work-graph --critical-path       # Show critical path
```

---

## Claude Behavior Guidelines

### Be Thorough

1. **Complete all tasks fully** - Don't rush through multiple items
2. **Don't make assumptions** - Ask clarifying questions when ambiguous
3. **Follow established patterns** - Check existing code before writing new code
4. **Validate your work** - Run `/validate` after making changes

### Ask Clarifying Questions When

- Requirements are vague or could be interpreted multiple ways
- You're unsure which of several approaches to take
- The task might affect other parts of the codebase
- You need to make architectural decisions

### Reference Documentation

- **ARCHITECTURE.md** - For architecture patterns and conventions
- **README.md** - For project-specific configuration
- **.session/specs/** - For work item requirements
- **Slash commands** (`/start`, `/end`, `/work-new`, etc.) - For Solokit operations

---

## What NOT to Do

1. **Don't edit tracking files directly**
   - NEVER edit `.session/tracking/work_items.json` manually
   - NEVER edit `.session/tracking/learnings.json` manually
   - Always use `sk` commands to modify these files

2. **Don't skip the spec file template**
   - ALWAYS use the template structure in `.session/specs/`
   - ALWAYS fill in all sections of the template

3. **Don't be inconsistent with multiple items**
   - If creating multiple work items, give equal attention to each
   - Each item deserves equal thoroughness

4. **Don't put learnings in wrong places**
   - NEVER add learnings to commit messages
   - ALWAYS use `/learn` or capture during `/end`

5. **Don't abandon sessions**
   - NEVER leave a session without running `/end`
   - ALWAYS complete the session workflow properly

6. **Don't skip quality gates**
   - NEVER commit code that fails linting or type checking
   - NEVER bypass pre-commit hooks with `--no-verify`

---

## Quick Reference

### Solokit Commands (Slash Commands)

| Command | Description |
|---------|-------------|
| `/work-list` | List all work items |
| `/work-show <id>` | Show work item details |
| `/work-new` | Create new work item |
| `/work-update <id>` | Update work item |
| `/work-delete <id>` | Delete work item |
| `/work-graph` | Visualize dependencies |
| `/work-next` | Get next recommended work item |
| `/start [id]` | Start a session |
| `/status` | Check session status |
| `/validate` | Validate quality gates |
| `/end` | End session |
| `/learn` | Capture a learning |
| `/learn-show` | View learnings |
| `/learn-search <query>` | Search learnings |

### Key Files

| File | Purpose |
|------|---------|
| `CLAUDE.md` | AI guidance (this file) |
| `ARCHITECTURE.md` | Architecture documentation |
| `README.md` | Project overview |
| `.session/tracking/work_items.json` | Work item data (use `sk` commands) |
| `.session/tracking/learnings.json` | Captured learnings (use `sk` commands) |
| `.session/specs/` | Work item specifications |
| `.session/briefings/` | Session briefings |
| `.session/history/` | Session summaries |
