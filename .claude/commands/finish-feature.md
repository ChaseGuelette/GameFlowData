---
description: Complete steps to safely end development of new features.  
---

# Ending new feature development

Check docs/development_docs folder to see if new sessions have run since starting development. 
Run pytest in order to ensure all things function properly. Update ARCHITECTURE.md, CHANGELOG.md, and ACTIONITEMS.md to maintain project context. 

## Step 1: Run Pytest

Run pytest in order to confirm new features didn't break old features. If pytest fails, prompt user to switch to /bash mode for further debugging
Run ruff check --fix. If there are remaining issues, like  unused variables or import issues, fix them. If it is a significant change, notify me

## Step 2: Update Architecture.md

Update the ARCHITECTURE.md file and explore the repository to clear up any ambiguities that you have

## Step 3: Update ActionItems.md

Update the ACTIONITEMS.md file and explore for further questinos

## Step 4: Update Changelog.md

Update the CHANGELOG.md file to see what happened in previous sessions. 

## Step 5: Update /docs for affected files 

Update the relevant .md files inthe /docs folder. Write new documentation for new files or features. 

## Step 6: Write development doc in /docs/development_docs folder

Write a session ending file in the docs/development_docs folder to with YYYY-MM-DD_SessionCount.md naming conventions. Use the existing structure for context. 