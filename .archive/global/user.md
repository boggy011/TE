# User-Level Claude Configuration

## Personal Preferences

### Code Style
- Always use type hints
- Use Google-style docstrings
- 4-space indentation

### Communication
- Be concise and direct
- Show code examples
- Explain trade-offs
- Flag security concerns immediately

### No AI Attribution Policy (CRITICAL)
**NEVER add "Claude", "Co-Authored-By: Claude", "AI-generated", or ANY AI attribution anywhere!**

This applies to:
- Code and comments
- Commit messages
- PR titles and descriptions
- Documentation
- Any other output

**This rule has NO exceptions!**

## Modular Rules

@rules/coding-style.md
@rules/testing.md
@rules/security.md
@rules/git-workflow.md
@rules/performance.md
@rules/patterns.md

## Available Agents

### Planning & Design (Opus)
- **planner**: Feature implementation planning
- **architect**: System design decisions
- **tdd-guide**: Test-driven development
- **code-reviewer**: Quality review
- **security-reviewer**: Vulnerability analysis

### Implementation (Sonnet)
- **pipeline-debugger**: Error resolution
- **data-quality-checker**: Data validation
- **notebook-optimizer**: Notebook refactoring
- **doc-updater**: Documentation sync

## Agent Usage

### Automatic (No prompt needed)
- Complex feature → Use planner first
- Any new code → Use tdd-guide
- After coding → Use code-reviewer
- Security code → Use security-reviewer

### On Request
- Architecture questions → architect
- Pipeline errors → pipeline-debugger
- Data issues → data-quality-checker
- Optimization → notebook-optimizer
- Documentation → doc-updater

## Commands

```
/plan        - Create implementation plan
/tdd         - Start TDD workflow
/code-review - Review code quality
/security-review - Security analysis
/data-quality - Run quality checks
/debug-pipeline - Debug errors
/optimize-notebook - Optimize notebook
/test-coverage - Check test coverage
/update-docs - Sync documentation
```

## Success Metrics

For every task:
- [ ] Tests pass with 80%+ coverage
- [ ] Code reviewed
- [ ] No security issues
- [ ] Documentation updated
- [ ] Performance acceptable

## Environment Setup

Required tools:
```bash
pip install mypy pytest pytest-cov
pip install pyspark delta-spark
```

## Databricks Configuration

```python
# Get environment
env = dbutils.widgets.get("environment")

# Get secrets
password = dbutils.secrets.get(
    scope=f"{env}-secrets",
    key="password"
)
```

## Notes

- Always prefer TDD approach
- Security is non-negotiable
- Document as you go
- Test edge cases
- Keep functions small
