#!/bin/bash
echo "BLOCKED: Direct Supabase call in main context. Delegate to sql-runner subagent instead." >&2
echo "Example: Task(subagent_type='general-purpose', model='haiku', prompt='Run this SQL: ...')" >&2
exit 2
