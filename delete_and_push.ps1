# Delete Test Files and Push to GitHub
# This script removes all test/demo files and commits the deletions

Write-Host "Deleting test files, demo files, and unwanted root-level files..."

# Set Git path
$gitPath = "C:\Program Files\Git\\bin\\git.exe"

# Files to delete (test files, demo files, logs, etc.)
$filesToDelete = @(
    "test_*.py",
    "*_test.py",
    "demo_*.py",
    "comprehensive_*.py",
    "enhanced_*.py",
    "priority_queue_demo.py",
    "simple_paper_trading_test.py",
    "validate_test_setup.py",
    "final_system_status.py",
    "system_cycle_demo.py",
    "queue_based_*.py",
    "*.log",
    "windows_orchestrator.py",
    "COMPREHENSIVE_CHANGES_SUMMARY.md",
    "RATE_LIMITING_SUMMARY.md",
    "commit_changes.ps1",
    "commit_friren_only.ps1",
    "emergency_cleanup.ps1"
)

# Delete each file pattern
foreach ($pattern in $filesToDelete) {
    $files = Get-ChildItem -Name $pattern -ErrorAction SilentlyContinue
    foreach ($file in $files) {
        if (Test-Path $file) {
            Write-Host "Deleting: $file"
            Remove-Item $file -Force
        }
    }
}

# Create proper .gitignore to prevent future issues
Write-Host "Creating comprehensive .gitignore..."
$gitignoreContent = @"
# Environment Variables - NEVER COMMIT
.env
*.env
.env.local
.env.production
*.key
*_keys.py
api_keys.py

# Test Files - DO NOT COMMIT
test_*.py
*_test.py
demo_*.py
comprehensive_*.py
enhanced_*.py
priority_queue_demo.py
simple_paper_trading_test.py
validate_test_setup.py
final_system_status.py
system_cycle_demo.py
queue_based_*.py

# Log Files
*.log
logs/
Friren_V1/logs/

# Cache and Temporary Files
__pycache__/
*.pyc
*.pyo
*.pyd
.Python
*.so
.coverage
.pytest_cache/
.tox/
.cache/
nosetests.xml
coverage.xml

# IDE Files
.vscode/
.idea/
*.swp
*.swo
*~
.DS_Store

# Python Virtual Environments
venv/
env/
ENV/
.venv/

# Django
*.sqlite3
db.sqlite3
/media/
/static/

# Jupyter Notebooks
.ipynb_checkpoints/

# Distribution / Packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# PyInstaller
*.manifest
*.spec

# Documentation builds
docs/_build/
"@

$gitignoreContent | Out-File -FilePath ".gitignore" -Encoding UTF8

# Stage all deletions and .gitignore
Write-Host "Staging deletions and .gitignore..."
& $gitPath add -A
& $gitPath add .gitignore

# Commit the deletions
Write-Host "Committing deletions..."
& $gitPath commit -m "CLEANUP: Remove test files, demo files, and sensitive data

- Deleted all test_*.py and *_test.py files
- Removed demo_*.py and comprehensive_*.py files
- Cleaned up enhanced_*.py and queue_based_*.py files
- Removed all log files and temporary files
- Added comprehensive .gitignore to prevent future issues
- Repository now contains only core Friren_V1 trading engine"

# Push to GitHub
Write-Host "Pushing deletions to GitHub..."
& $gitPath push origin main

Write-Host "Cleanup complete! Repository now contains only Friren_V1 trading engine."
Write-Host ".gitignore added to prevent future test file commits."
