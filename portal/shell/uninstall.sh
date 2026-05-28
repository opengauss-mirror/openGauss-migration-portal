#!/bin/bash

# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
# Description: Uninstall script for Portal

set -euo pipefail

PROJECT_DIR=""
INSTALL_DIR=""
REMOVE_PACKAGE=false

usage() {
    echo "Usage: $0 [--remove-package]"
    echo "Uninstall Portal and related components."
    echo
    echo "Options:"
    echo "  --remove-package   Remove the installation package (PortalControl-*.tar.gz) after uninstallation"
    echo "  -h, --help         Display this help message"
}

parse_arguments() {
    if [ $# -gt 1 ]; then
        echo "Error: Only empty or one argument is allowed"
        usage
        exit 1
    fi

    if [[ $# -gt 0 ]]; then
        case "$1" in
            --remove-package)
                REMOVE_PACKAGE=true
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                echo "Error: Invalid argument '$1'"
                usage
                exit 1
                ;;
        esac
    fi
}

clear_chameleon_tmp() {
    echo "Clearing chameleon temporary files..."

    local chameleon_dir
    chameleon_dir=$(find "$PROJECT_DIR/tools/chameleon" -maxdepth 1 -type d -name "chameleon-*" 2>/dev/null | head -n1 || true)
    echo "Chameleon directory: $chameleon_dir"
    
    if [[ -z "$chameleon_dir" ]]; then
        echo "Warning: No chameleon directory found in $PROJECT_DIR/tools/chameleon"
        return 0
    fi

    cd "$chameleon_dir" || {
        echo "Warning: Failed to change to chameleon directory: $chameleon_dir"
        return 1
    }

    local clear_script="clear_env_var.sh"
    if [ -f "$clear_script" ]; then
        bash "$clear_script" || {
            echo "Warning: Failed to execute $clear_script in $chameleon_dir"
            return 1
        }
    else
        echo "Warning: $clear_script not found in $chameleon_dir"
    fi
}

kill_portal_relevant_processes() {
    echo "Killing portal relevant processes..."

    local pids
    pids=$(ps ux | grep "$PROJECT_DIR" | grep -v "$0" | awk '{print $2}')

    if [[ -n "$pids" ]]; then
        kill -9 $pids 2> /dev/null || true
    else
        echo "No portal relevant processes found"
    fi
}

remove_install_dir() {
    echo "Removing install directory and logs..."

    if [[ -z "$INSTALL_DIR" ]] || [[ -z "$PROJECT_DIR" ]]; then
        echo "Error: INSTALL_DIR or PROJECT_DIR is not set"
        exit 1
    fi

    cd "$INSTALL_DIR" || {
        echo "Warning: Failed to change to INSTALL_DIR: $INSTALL_DIR, skip removing install directory and logs"
        return 1
    }

    if [[ -d "$PROJECT_DIR" ]]; then
        rm -rf "$PROJECT_DIR"
    fi

    local datakit_install_log="datakit_install_portal.log"
    if [[ -f "$datakit_install_log" ]]; then
        rm -f $datakit_install_log
    fi
}

remove_install_package() {
    echo "Removing install package..."

    cd "$INSTALL_DIR" || {
        echo "Warning: Failed to change to INSTALL_DIR: $INSTALL_DIR, skip removing install package"
        return 1
    }

    local package_pattern="PortalControl-*.tar.gz"
    rm -rf $package_pattern
}

main() {
    parse_arguments "$@"

    echo "Starting Portal uninstallation..."

    PROJECT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
    INSTALL_DIR=$(dirname "$PROJECT_DIR")
    echo "Project directory: $PROJECT_DIR"
    echo "Install directory: $INSTALL_DIR"

    if [[ ! -d "$PROJECT_DIR" ]]; then
        echo "Error: Project directory does not exist: $PROJECT_DIR"
        exit 1
    fi

    read -p "Are you sure you want to uninstall? This will remove all files in $PROJECT_DIR [y/n]: " -r confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "Uninstallation cancelled"
        exit 0
    fi
    
    clear_chameleon_tmp || true
    kill_portal_relevant_processes
    remove_install_dir
    if [[ $REMOVE_PACKAGE == true ]]; then
        remove_install_package
    fi

    echo "Uninstallation completed successfully!"
}

main "$@"