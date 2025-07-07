#!/usr/bin/env python3
"""
Setup Verification Script
Run this to check if your environment is properly configured
"""

import sys
import subprocess
import os

def check_python_version():
    """Check if Python version is compatible"""
    print("🔍 Checking Python version...")
    version = sys.version_info
    print(f"   Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 7):
        print("❌ Python 3.7+ required. Please upgrade Python.")
        return False
    else:
        print("✅ Python version is compatible")
        return True

def check_virtual_environment():
    """Check if virtual environment is active"""
    print("\n🔍 Checking virtual environment...")
    
    venv_active = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    
    if venv_active:
        print("✅ Virtual environment is active")
        print(f"   Environment path: {sys.prefix}")
        return True
    else:
        print("⚠️  Virtual environment not detected")
        print("   Consider activating your virtual environment first")
        return False

def check_package_installation():
    """Check if required packages are installed"""
    print("\n🔍 Checking package installations...")
    
    packages = {
        'google.generativeai': 'google-generativeai',
        'mysql.connector': 'mysql-connector-python', 
        'dotenv': 'python-dotenv'
    }
    
    results = {}
    
    for import_name, package_name in packages.items():
        try:
            __import__(import_name)
            print(f"✅ {package_name} - installed and importable")
            results[package_name] = True
        except ImportError:
            print(f"❌ {package_name} - not installed or not importable")
            results[package_name] = False
    
    return results

def test_google_generativeai():
    """Test basic Google Generative AI functionality"""
    print("\n🔍 Testing Google Generative AI import...")
    
    try:
        import google.generativeai as genai
        print("✅ google.generativeai imported successfully")
        
        # Try to get the version
        try:
            version = genai.__version__
            print(f"   Version: {version}")
        except:
            print("   Version info not available")
        
        return True
        
    except ImportError as e:
        print(f"❌ Failed to import google.generativeai: {e}")
        print("   This is the main issue that needs to be fixed")
        return False

def check_environment_variables():
    """Check for required environment variables"""
    print("\n🔍 Checking environment variables...")
    
    required_vars = ['GEMINI_API_KEY', 'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    
    # Try to load from .env file
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ .env file loaded")
    except:
        print("⚠️  Could not load .env file")
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if value:
            print(f"✅ {var} - found")
        else:
            print(f"❌ {var} - missing")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n⚠️  Missing environment variables: {', '.join(missing_vars)}")
        print("   Create a .env file with these variables")
    
    return len(missing_vars) == 0

def main():
    """Main verification function"""
    print("🚀 Product AI Enhancement System - Setup Verification")
    print("="*60)
    
    # Run all checks
    python_ok = check_python_version()
    venv_active = check_virtual_environment()
    
    package_results = check_package_installation()
    failed_packages = [pkg for pkg, success in package_results.items() if not success]
    
    google_ai_ok = test_google_generativeai()
    env_vars_ok = check_environment_variables()
    
    # Summary
    print("\n" + "="*60)
    print("📋 SUMMARY")
    print("="*60)
    
    if python_ok and google_ai_ok and not failed_packages and env_vars_ok:
        print("🎉 EVERYTHING IS WORKING!")
        print("   You can now run the Product AI Enhancement System")
    else:
        print("❌ ISSUES FOUND:")
        
        if not python_ok:
            print("   - Python version needs to be 3.7+")
        
        if failed_packages:
            print(f"   - Missing packages: {', '.join(failed_packages)}")
            print("   - Run: pip install " + " ".join(failed_packages))
        
        if not google_ai_ok:
            print("   - google.generativeai import failed")
            
        if not env_vars_ok:
            print("   - Environment variables missing - create .env file")

if __name__ == "__main__":
    main()