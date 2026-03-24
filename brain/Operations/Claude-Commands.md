# Claude Commands

> Part of [[Operations]]

Slash commands registered in `.claude/commands/` that provide structured workflows for Claude Code sessions. Invoked via `/command-name` in the Claude Code CLI.

## Solokit Session Management

- [[start]] - Begin a new development session with a comprehensive briefing, work item selection, and implementation guidelines
- [[end]] - Complete the current session with quality gates, changelog updates, learning capture, and optional PR creation
- [[status]] - Display current session status including work item progress, time elapsed, git info, and milestone progress
- [[validate]] - Run quality checks (tests, linting, formatting, coverage) without ending the session; supports `--fix` for auto-repair
- [[init]] - Initialize a new Solokit project from scratch with template-based setup (SaaS, ML/AI, Dashboard, Full-Stack)
- [[adopt]] - Add Solokit session management to an existing project without modifying source code

## Work Item Management

- [[work-new]] - Create a new work item interactively with type, title, priority, urgency, and dependency selection
- [[work-list]] - List all work items with optional filtering by status, type, or milestone
- [[work-show]] - Show detailed information about a specific work item including dependencies, session history, and spec preview
- [[work-update]] - Update work item fields such as status, priority, milestone, dependencies, or urgency
- [[work-delete]] - Delete a work item from the system with optional spec file removal and dependency warnings
- [[work-next]] - Get the next recommended work item based on dependency resolution and priority ranking
- [[work-graph]] - Generate dependency graph visualizations with critical path, bottleneck analysis, and filtering options

## Learning System

- [[learn]] - Capture insights, gotchas, and best practices from the current session with AI-suggested categorization
- [[learn-show]] - Browse and filter captured learnings by category, tag, or session number
- [[learn-search]] - Full-text search across all learning content, tags, context, and category names
- [[learn-curate]] - Run automatic categorization, duplicate detection, merging, and archiving of learnings

## Claude-Flow Orchestration

- [[claude-flow-help]] - Reference guide for all Claude-Flow commands: system management, agents, tasks, memory, SPARC, swarms, and MCP
- [[claude-flow-memory]] - Interact with the Claude-Flow persistent memory system for cross-session and cross-agent collaboration
- [[swarm]] - Coordinate multi-agent swarms for development, research, analysis, testing, and optimization tasks

## Project Development Workflow

- [[start-development]] - Intake project context by reading ARCHITECTURE.md, CHANGELOG.md, ACTIONITEMS.md, and development docs
- [[finish-feature]] - Safely end feature development with pytest, ruff, doc updates, and a session-ending development doc

## Model Operations

- [[check-calibration]] - Run a full calibration health check comparing production drift, paper trading ROI, and training baseline with a recommended action

---

## SPARC Framework Commands

Located in `.claude/commands/sparc/`. These provide specialized AI agent modes for different development tasks.

- [[sparc]] - Core SPARC methodology: Specification, Pseudocode, Architecture, Refinement, Completion
- [[sparc-modes]] - Overview of all available SPARC operational modes
- [[spec-pseudocode]] - Generate specification and pseudocode for a feature before implementation
- [[architect]] - System architecture design and technical decision-making
- [[analyzer]] - Code analysis, complexity assessment, and pattern detection
- [[ask]] - Interactive Q&A mode for exploring problems and solutions
- [[batch-executor]] - Execute multiple tasks in sequence or parallel
- [[code]] - Direct code generation and implementation
- [[coder]] - Extended coding mode with context-aware implementation
- [[debug]] - Systematic debugging with root cause analysis
- [[debugger]] - Advanced debugging with step-through and state inspection
- [[designer]] - UI/UX design patterns and component architecture
- [[devops]] - Infrastructure, CI/CD, and deployment automation
- [[docs-writer]] - Technical documentation generation
- [[documenter]] - Comprehensive documentation with API references
- [[innovator]] - Creative problem-solving and novel approach generation
- [[integration]] - System integration, API connections, and data flow design
- [[mcp]] - Model Context Protocol server management and configuration
- [[memory-manager]] - Session and cross-session memory management
- [[optimizer]] - Performance optimization and resource efficiency
- [[orchestrator]] - Multi-agent task coordination and workflow management
- [[post-deployment-monitoring-mode]] - Post-deploy health checks and monitoring setup
- [[refinement-optimization-mode]] - Iterative code refinement and optimization cycles
- [[researcher]] - Research mode for exploring technologies and solutions
- [[reviewer]] - Code review with best practices and security checks
- [[security-review]] - Security audit and vulnerability assessment
- [[supabase-admin]] - Supabase-specific administration and configuration
- [[swarm-coordinator]] - Multi-agent swarm coordination and task distribution
- [[tdd]] - Test-driven development workflow
- [[tester]] - Test generation, execution, and coverage analysis
- [[tutorial]] - Interactive tutorial and learning mode
- [[workflow-manager]] - Complex workflow design and execution management

## GitHub Commands

Located in `.claude/commands/github/`. GitHub-specific automation and workflow tools.

- [[github-modes]] - Overview of all GitHub operational modes
- README (`.claude/commands/github/README.md`) - GitHub commands reference
- [[github-swarm]] - Multi-agent GitHub workflow coordination
- [[code-review]] - Structured code review workflow
- [[code-review-swarm]] - Multi-agent code review for large PRs
- [[issue-tracker]] - GitHub issue tracking and management
- [[issue-triage]] - Automated issue triage and prioritization
- [[multi-repo-swarm]] - Cross-repository coordination and management
- [[pr-enhance]] - Pull request enhancement and optimization
- [[pr-manager]] - Full PR lifecycle management
- [[project-board-sync]] - Sync project boards with issues and PRs
- [[release-manager]] - Release planning, tagging, and changelog generation
- [[release-swarm]] - Multi-agent release coordination
- [[repo-analyze]] - Repository health analysis and metrics
- [[repo-architect]] - Repository structure design and reorganization
- [[swarm-issue]] - Swarm-based issue resolution
- [[swarm-pr]] - Swarm-based PR creation and review
- [[sync-coordinator]] - Cross-repo synchronization management
- [[workflow-automation]] - GitHub Actions workflow design and automation

## Monitoring Commands

Located in `.claude/commands/monitoring/`. System and agent monitoring tools.

- [[agent-metrics]] - Agent performance metrics and analytics
- README (`.claude/commands/monitoring/README.md`) - Monitoring commands reference
- [[agents]] - List and manage active agents
- [[real-time-view]] - Real-time system status dashboard
- [[swarm-monitor]] - Monitor active swarm operations

## Automation Commands

Located in `.claude/commands/automation/`. Automated workflow and agent management.

- [[auto-agent]] - Self-configuring agent for autonomous task execution
- README (`.claude/commands/automation/README.md`) - Automation commands reference
- [[self-healing]] - Automatic error detection and recovery
- [[session-memory]] - Persistent session state and memory management
- [[smart-agents]] - Intelligent agent spawning based on task analysis
- [[smart-spawn]] - Context-aware agent creation with optimal configuration
- [[workflow-select]] - Interactive workflow selection and configuration

## Optimization Commands

Located in `.claude/commands/optimization/`. Performance and resource optimization.

- [[auto-topology]] - Automatic agent topology optimization
- README (`.claude/commands/optimization/README.md`) - Optimization commands reference
- [[cache-manage]] - Cache strategy management and optimization
- [[parallel-execute]] - Parallel task execution management
- [[parallel-execution]] - Advanced parallel execution with dependency resolution
- [[topology-optimize]] - Agent topology optimization for throughput

## Analysis Commands

Located in `.claude/commands/analysis/`. Code and performance analysis tools.

- [[bottleneck-detect]] - System bottleneck detection and analysis
- [[COMMAND_COMPLIANCE_REPORT]] - Command system compliance audit
- README (`.claude/commands/analysis/README.md`) - Analysis commands reference
- [[performance-bottlenecks]] - Detailed performance bottleneck profiling
- [[performance-report]] - Comprehensive performance reporting
- [[token-efficiency]] - Token usage efficiency analysis
- [[token-usage]] - Detailed token consumption tracking

## Hooks Commands

Located in `.claude/commands/hooks/`. Event-driven automation hooks.

- [[overview]] - Hooks system overview and available events
- README (`.claude/commands/hooks/README.md`) - Hooks commands reference
- [[pre-edit]] - Pre-edit validation and checks
- [[post-edit]] - Post-edit automation triggers
- [[pre-task]] - Pre-task setup and validation
- [[post-task]] - Post-task cleanup and reporting
- [[session-end]] - End-of-session automation
- [[setup]] - Hooks system installation and configuration
