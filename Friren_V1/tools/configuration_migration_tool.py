"""
Configuration Migration Tool

Automates the process of replacing hardcoded values with configuration-managed values
throughout the Friren trading system codebase.

Features:
- Scans for hardcoded sleep/timeout values
- Replaces with configuration-managed equivalents
- Validates configuration schema compliance
- Generates migration reports
"""

import os
import re
import ast
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
import json

# Import configuration manager
try:
    from Friren_V1.infrastructure.configuration_manager import (
        ConfigurationManager, get_config_manager, get_config
    )
    HAS_CONFIG_MANAGER = True
except ImportError:
    HAS_CONFIG_MANAGER = False


@dataclass
class HardcodedValue:
    """Represents a hardcoded value found in code"""
    file_path: str
    line_number: int
    line_content: str
    value: Any
    value_type: str
    context: str
    suggested_config: str
    suggested_replacement: str


@dataclass
class MigrationResult:
    """Result of migration operation"""
    file_path: str
    original_content: str
    modified_content: str
    changes_made: List[str]
    success: bool
    error_message: Optional[str] = None


class ConfigurationMigrationTool:
    """
    Tool for migrating hardcoded values to configuration management
    """
    
    def __init__(self, project_root: str):
        self.project_root = Path(project_root)
        self.logger = logging.getLogger("config_migration")
        
        # Patterns for detecting hardcoded values
        self.patterns = {
            'sleep_calls': [
                r'time\.sleep\((\d+(?:\.\d+)?)\)',
                r'asyncio\.sleep\((\d+(?:\.\d+)?)\)',
            ],
            'timeout_values': [
                r'timeout\s*=\s*(\d+(?:\.\d+)?)',
                r'socket_timeout\s*=\s*(\d+(?:\.\d+)?)',
                r'connect_timeout\s*=\s*(\d+(?:\.\d+)?)',
            ],
            'redis_params': [
                r'host\s*=\s*[\'"]localhost[\'"]',
                r'port\s*=\s*6379',
                r'db\s*=\s*0',
            ],
            'model_paths': [
                r'[\'"]models/[^\'\"]*\.(?:json|pkl|model)[\'"]',
            ],
            'thresholds': [
                r'threshold\s*=\s*(\d+(?:\.\d+)?)',
                r'_threshold\s*:\s*float\s*=\s*(\d+(?:\.\d+)?)',
            ]
        }
        
        # Configuration mappings for common values
        self.value_mappings = {
            # Sleep durations
            '3': 'STARTUP_DELAY_SECONDS',
            '5': 'RETRY_DELAY_SECONDS', 
            '10': 'PROCESS_MONITOR_INTERVAL_SECONDS',
            '30': 'HEALTH_CHECK_INTERVAL_SECONDS',
            '60': 'HEALTH_CHECK_INTERVAL_SECONDS',
            
            # Redis parameters
            'localhost': 'REDIS_HOST',
            '6379': 'REDIS_PORT',
            '0': 'REDIS_DB',
            
            # Common thresholds
            '0.65': 'XGBOOST_BUY_THRESHOLD',
            '0.35': 'XGBOOST_SELL_THRESHOLD',
            '0.5': 'FINBERT_CONFIDENCE_THRESHOLD',
        }
        
        self.found_values: List[HardcodedValue] = []
        self.migration_results: List[MigrationResult] = []
    
    def scan_for_hardcoded_values(self, file_extensions: List[str] = ['.py']) -> List[HardcodedValue]:
        """Scan project for hardcoded values"""
        self.found_values = []
        
        for ext in file_extensions:
            pattern = f"**/*{ext}"
            for file_path in self.project_root.rglob(pattern):
                if self._should_skip_file(file_path):
                    continue
                
                try:
                    self._scan_file(file_path)
                except Exception as e:
                    self.logger.warning(f"Error scanning {file_path}: {e}")
        
        self.logger.info(f"Found {len(self.found_values)} hardcoded values")
        return self.found_values
    
    def _should_skip_file(self, file_path: Path) -> bool:
        """Check if file should be skipped"""
        skip_patterns = [
            '__pycache__',
            '.git',
            '.venv',
            'venv',
            'node_modules',
            '.pytest_cache',
            'build',
            'dist',
            '.env'
        ]
        
        path_str = str(file_path)
        return any(pattern in path_str for pattern in skip_patterns)
    
    def _scan_file(self, file_path: Path):
        """Scan individual file for hardcoded values"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            for line_num, line in enumerate(lines, 1):
                self._scan_line(file_path, line_num, line)
                
        except Exception as e:
            self.logger.warning(f"Could not read {file_path}: {e}")
    
    def _scan_line(self, file_path: Path, line_num: int, line: str):
        """Scan individual line for hardcoded values"""
        line_stripped = line.strip()
        
        # Skip comments and empty lines
        if not line_stripped or line_stripped.startswith('#'):
            return
        
        # Check each pattern category
        for category, patterns in self.patterns.items():
            for pattern in patterns:
                matches = re.finditer(pattern, line)
                for match in matches:
                    self._process_match(file_path, line_num, line, match, category)
    
    def _process_match(self, file_path: Path, line_num: int, line: str, match: re.Match, category: str):
        """Process a pattern match"""
        matched_value = match.group(1) if match.groups() else match.group(0)
        
        # Determine suggested configuration
        suggested_config = self._get_suggested_config(matched_value, category, line)
        suggested_replacement = self._get_suggested_replacement(matched_value, category, line)
        
        hardcoded_value = HardcodedValue(
            file_path=str(file_path.relative_to(self.project_root)),
            line_number=line_num,
            line_content=line.strip(),
            value=matched_value,
            value_type=category,
            context=self._get_context(line),
            suggested_config=suggested_config,
            suggested_replacement=suggested_replacement
        )
        
        self.found_values.append(hardcoded_value)
    
    def _get_suggested_config(self, value: str, category: str, line: str) -> str:
        """Get suggested configuration name for a value"""
        # Direct mapping
        if value in self.value_mappings:
            return self.value_mappings[value]
        
        # Category-based suggestions
        if category == 'sleep_calls':
            if 'startup' in line.lower() or 'init' in line.lower():
                return 'STARTUP_DELAY_SECONDS'
            elif 'retry' in line.lower():
                return 'RETRY_DELAY_SECONDS'
            elif 'health' in line.lower() or 'monitor' in line.lower():
                return 'HEALTH_CHECK_INTERVAL_SECONDS'
            else:
                return 'PROCESS_MONITOR_INTERVAL_SECONDS'
        
        elif category == 'timeout_values':
            if 'redis' in line.lower():
                return 'REDIS_CONNECT_TIMEOUT'
            elif 'socket' in line.lower():
                return 'REDIS_SOCKET_TIMEOUT'
            else:
                return 'DEFAULT_TIMEOUT_SECONDS'
        
        elif category == 'redis_params':
            if 'host' in line.lower():
                return 'REDIS_HOST'
            elif 'port' in line.lower():
                return 'REDIS_PORT'
            elif 'db' in line.lower():
                return 'REDIS_DB'
        
        elif category == 'model_paths':
            if 'xgboost' in line.lower() or 'xgb' in line.lower():
                return 'XGBOOST_MODEL_PATH'
            else:
                return 'MODEL_PATH'
        
        elif category == 'thresholds':
            if 'buy' in line.lower():
                return 'XGBOOST_BUY_THRESHOLD'
            elif 'sell' in line.lower():
                return 'XGBOOST_SELL_THRESHOLD'
            elif 'confidence' in line.lower():
                return 'FINBERT_CONFIDENCE_THRESHOLD'
            else:
                return 'THRESHOLD_VALUE'
        
        return f"CONFIG_{category.upper()}_{value.replace('.', '_')}"
    
    def _get_suggested_replacement(self, value: str, category: str, line: str) -> str:
        """Get suggested code replacement"""
        config_name = self._get_suggested_config(value, category, line)
        
        if category == 'sleep_calls':
            return f"get_config('{config_name}')"
        elif category in ['timeout_values', 'thresholds']:
            return f"get_config('{config_name}')"
        elif category == 'redis_params':
            if value == 'localhost':
                return f"get_config('{config_name}')"
            else:
                return f"get_config('{config_name}')"
        elif category == 'model_paths':
            return f"get_config('{config_name}')"
        
        return f"get_config('{config_name}')"
    
    def _get_context(self, line: str) -> str:
        """Extract context from line"""
        # Extract function or class context
        if 'def ' in line:
            return 'function_definition'
        elif 'class ' in line:
            return 'class_definition'
        elif '=' in line:
            return 'assignment'
        elif 'time.sleep' in line:
            return 'sleep_call'
        elif 'timeout' in line:
            return 'timeout_setting'
        else:
            return 'other'
    
    def generate_migration_report(self, output_file: Optional[str] = None) -> str:
        """Generate detailed migration report"""
        if not self.found_values:
            return "No hardcoded values found."
        
        report = []
        report.append("# Configuration Migration Report")
        report.append("=" * 50)
        report.append(f"Total hardcoded values found: {len(self.found_values)}")
        report.append("")
        
        # Group by category
        by_category = {}
        for value in self.found_values:
            category = value.value_type
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(value)
        
        for category, values in by_category.items():
            report.append(f"## {category.upper()} ({len(values)} instances)")
            report.append("")
            
            for value in values:
                report.append(f"**File:** {value.file_path}:{value.line_number}")
                report.append(f"**Line:** `{value.line_content}`")
                report.append(f"**Value:** {value.value}")
                report.append(f"**Suggested Config:** {value.suggested_config}")
                report.append(f"**Suggested Replacement:** {value.suggested_replacement}")
                report.append("")
        
        # Required environment variables
        report.append("## Required Environment Variables")
        report.append("")
        unique_configs = set(v.suggested_config for v in self.found_values)
        for config in sorted(unique_configs):
            report.append(f"- {config}")
        
        report_text = "\n".join(report)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report_text)
            self.logger.info(f"Migration report saved to {output_file}")
        
        return report_text
    
    def auto_migrate_file(self, file_path: str, dry_run: bool = True) -> MigrationResult:
        """Automatically migrate hardcoded values in a file"""
        file_path = Path(file_path)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            modified_content = original_content
            changes_made = []
            
            # Get hardcoded values for this file
            file_values = [v for v in self.found_values if v.file_path == str(file_path.relative_to(self.project_root))]
            
            for value in file_values:
                # Simple replacement - this could be more sophisticated
                old_pattern = value.value
                new_replacement = value.suggested_replacement
                
                if old_pattern in modified_content:
                    modified_content = modified_content.replace(old_pattern, new_replacement)
                    changes_made.append(f"Replaced {old_pattern} with {new_replacement}")
            
            # Add import if modifications were made
            if changes_made and 'get_config' in modified_content:
                import_line = "from Friren_V1.infrastructure.configuration_manager import get_config\n"
                if import_line not in modified_content:
                    # Add import at top of file after existing imports
                    lines = modified_content.split('\n')
                    import_index = 0
                    for i, line in enumerate(lines):
                        if line.startswith('import ') or line.startswith('from '):
                            import_index = i + 1
                    
                    lines.insert(import_index, import_line)
                    modified_content = '\n'.join(lines)
                    changes_made.append("Added configuration manager import")
            
            if not dry_run and changes_made:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(modified_content)
            
            return MigrationResult(
                file_path=str(file_path),
                original_content=original_content,
                modified_content=modified_content,
                changes_made=changes_made,
                success=True
            )
            
        except Exception as e:
            return MigrationResult(
                file_path=str(file_path),
                original_content="",
                modified_content="",
                changes_made=[],
                success=False,
                error_message=str(e)
            )
    
    def validate_configuration_coverage(self) -> Dict[str, Any]:
        """Validate that all suggested configurations exist in config manager"""
        if not HAS_CONFIG_MANAGER:
            return {"error": "Configuration manager not available"}
        
        config_manager = get_config_manager()
        all_schemas = config_manager.get_all_schemas()
        
        suggested_configs = set(v.suggested_config for v in self.found_values)
        missing_configs = []
        existing_configs = []
        
        for config_name in suggested_configs:
            if config_name in all_schemas:
                existing_configs.append(config_name)
            else:
                missing_configs.append(config_name)
        
        return {
            "total_suggested": len(suggested_configs),
            "existing_configs": existing_configs,
            "missing_configs": missing_configs,
            "coverage_percentage": (len(existing_configs) / len(suggested_configs)) * 100 if suggested_configs else 100
        }


def main():
    """Main function for running the migration tool"""
    project_root = "/mnt/c/users/usose/onedrive/desktop/projects/project-friren"
    
    print("🔧 Configuration Migration Tool")
    print("=" * 40)
    
    # Initialize tool
    tool = ConfigurationMigrationTool(project_root)
    
    # Scan for hardcoded values
    print("📊 Scanning for hardcoded values...")
    hardcoded_values = tool.scan_for_hardcoded_values()
    
    if not hardcoded_values:
        print("✅ No hardcoded values found!")
        return
    
    print(f"Found {len(hardcoded_values)} hardcoded values")
    
    # Generate report
    print("📄 Generating migration report...")
    report = tool.generate_migration_report("migration_report.md")
    
    # Validate configuration coverage
    print("🔍 Validating configuration coverage...")
    coverage = tool.validate_configuration_coverage()
    print(f"Configuration coverage: {coverage.get('coverage_percentage', 0):.1f}%")
    
    if coverage.get('missing_configs'):
        print("Missing configurations:")
        for config in coverage['missing_configs']:
            print(f"  - {config}")
    
    # Show summary by category
    by_category = {}
    for value in hardcoded_values:
        category = value.value_type
        by_category[category] = by_category.get(category, 0) + 1
    
    print("\n📈 Summary by category:")
    for category, count in sorted(by_category.items()):
        print(f"  {category}: {count} instances")
    
    print(f"\n📋 Full report saved to: migration_report.md")


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    main()