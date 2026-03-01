#!/usr/bin/env python3
"""
🚂 Dutch Railway Analytics - Cross-Platform Setup Script

Simple setup script that works on Mac, Windows, and Linux.
"""

import os
import sys
import subprocess
import platform
import shutil
from pathlib import Path


class Colors:
    """ANSI color codes for cross-platform terminal output"""
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color
    
    @classmethod
    def disable_on_windows(cls):
        """Disable colors on Windows if not supported"""
        if platform.system() == 'Windows' and not os.environ.get('ANSICON'):
            cls.GREEN = cls.YELLOW = cls.RED = cls.BLUE = cls.NC = ''


def print_colored(message, color=Colors.NC):
    """Print message with color support"""
    print(f"{color}{message}{Colors.NC}")


def run_command(cmd, cwd=None, check=True):
    """Run a command and handle errors gracefully"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            cwd=cwd, 
            check=check,
            capture_output=True,
            text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        print_colored(f"❌ Command failed: {cmd}", Colors.RED)
        print_colored(f"Return code: {e.returncode}", Colors.RED)
        if e.stdout:
            print_colored(f"stdout: {e.stdout}", Colors.RED)
        if e.stderr:
            print_colored(f"stderr: {e.stderr}", Colors.RED)
        sys.exit(1)


def check_command_exists(command):
    """Check if a command exists in PATH"""
    return shutil.which(command) is not None


def get_python_executable():
    """Find the appropriate Python executable"""
    # Try different Python command names
    for python_cmd in ['python3', 'python', 'py']:
        if check_command_exists(python_cmd):
            try:
                result = subprocess.run([python_cmd, '--version'], 
                                      capture_output=True, text=True)
                if result.returncode == 0:
                    version_info = result.stdout.strip()
                    print_colored(f"✅ Found {version_info}", Colors.GREEN)
                    return python_cmd
            except:
                continue
    
    print_colored("❌ Python not found. Please install Python 3.10+", Colors.RED)
    print("Visit: https://www.python.org/")
    sys.exit(1)


def install_uv():
    """Install uv package manager"""
    if check_command_exists('uv'):
        print_colored("✅ uv found", Colors.GREEN)
        return
    
    print_colored("📦 Installing uv...", Colors.YELLOW)
    
    system = platform.system().lower()
    
    if system in ['darwin', 'linux']:
        # Mac and Linux
        cmd = 'curl -LsSf https://astral.sh/uv/install.sh | sh'
        run_command(cmd)
        
        # Add to PATH for current session
        cargo_bin = os.path.expanduser("~/.cargo/bin")
        if cargo_bin not in os.environ.get('PATH', ''):
            os.environ['PATH'] = f"{cargo_bin}:{os.environ.get('PATH', '')}"
            
    elif system == 'windows':
        # Windows
        try:
            # Try PowerShell method first
            cmd = 'powershell -c "irm https://astral.sh/uv/install.ps1 | iex"'
            run_command(cmd)
        except:
            # Fallback to pip if available
            python_cmd = get_python_executable()
            run_command(f"{python_cmd} -m pip install uv")
    
    # Verify installation
    if not check_command_exists('uv'):
        print_colored("❌ Failed to install uv. Please install manually:", Colors.RED)
        print("Visit: https://docs.astral.sh/uv/getting-started/installation/")
        sys.exit(1)
    
    print_colored("✅ uv installed successfully", Colors.GREEN)


def main():
    """Main setup function"""
    # Handle colors on Windows
    Colors.disable_on_windows()
    
    print_colored("🚂 Dutch Railway Analytics - Simple Setup", Colors.BLUE)
    print_colored("=========================================", Colors.BLUE)
    print()
    
    # Check prerequisites
    print_colored("📋 Checking prerequisites...", Colors.BLUE)
    
    # Check Python
    python_cmd = get_python_executable()
    
    # Check/Install uv
    install_uv()
    
    print()
    print_colored("🏗️  Setting up the environment...", Colors.BLUE)
    
    # Install Python dependencies
    print_colored("📦 Installing Python dependencies...", Colors.YELLOW)
    run_command("uv sync")
    
    print_colored("🔧 Building dbt models...", Colors.BLUE)
    print()
    
    # Change to dutch_railway directory
    dutch_railway_dir = Path("dutch_railway")
    if not dutch_railway_dir.exists():
        print_colored("❌ dutch_railway directory not found", Colors.RED)
        sys.exit(1)
    
    # Install dbt dependencies
    print_colored("📦 Installing dbt dependencies...", Colors.YELLOW)
    run_command("uv run dbt deps --profiles-dir .", cwd=dutch_railway_dir)
    
    # Build staging layer
    print_colored("🏗️  Running dbt staging models...", Colors.YELLOW)
    run_command("uv run dbt build --select +staging --profiles-dir .", cwd=dutch_railway_dir)
    
    print()
    print_colored("🎉 Setup Complete!", Colors.GREEN)
    print()
    print_colored("🔎 See README.md for next steps", Colors.BLUE)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_colored("\n❌ Setup interrupted by user", Colors.RED)
        sys.exit(1)
    except Exception as e:
        print_colored(f"❌ Unexpected error: {str(e)}", Colors.RED)
        sys.exit(1)
