#!/bin/bash

valid_system_archs=("CentOS7-x86_64" "openEuler20.03-x86_64" "openEuler20.03-aarch64" "openEuler22.03-x86_64" "openEuler22.03-aarch64" "openEuler24.03-x86_64" "openEuler24.03-aarch64")

usage() {
    temp=""

    for ((i=0; i<${#valid_system_archs[@]}; i++))
    do
        if [ $i -eq 0 ]; then
            temp="${valid_system_archs[i]}"
        else
            temp="${temp}|${valid_system_archs[i]}"
        fi
    done

    echo "Usage: $0 <${temp}>"
    exit 1
}

check_param() {
    if [ $# -eq 0 ]; then
        echo "No arguments provided"
        usage
    fi

    if [ $# -gt 1 ]; then
        echo "Too many arguments provided"
        usage
    fi

    if [[ ! " ${valid_system_archs[@]} " =~ " $1 " ]]; then
        echo "The '$1' parameter is invalid."
        usage
    fi
}

config_properties() {
    system_arch=$1

    IFS='-' read -ra parts <<< "$system_arch"
    if [[ ${#parts[@]} -ne 2 ]]; then
        echo "The '$1' parameter is invalid."
        exit 1
    fi

    echo "system.name=${parts[0]}" > portal/config/application.properties
    echo "system.arch=${parts[1]}" >> portal/config/application.properties
    echo " Portal config file generated successfully"
}

download_dependencies() {
    local base_dir="../portal/offline/install"
    local target_dir="../../../multidb-portal/portal"
    local platform="$1"
    local script_args=()

    if ! cd "${base_dir}"; then
        echo "Error: Failed to enter directory ${base_dir}" >&2
        exit 1;
    fi

    echo "Start to download the RPM packages"

    case "$platform" in
        "CentOS7-x86_64")
            script_args=("CentOS7_x86_64" "$target_dir")
            ;;
        "openEuler20.03-x86_64")
            script_args=("openEuler2003_x86_64" "$target_dir")
            ;;
        "openEuler20.03-aarch64")
            script_args=("openEuler2003_aarch64" "$target_dir")
            ;;
        "openEuler22.03-x86_64")
            script_args=("openEuler2203_x86_64" "$target_dir")
            ;;
        "openEuler22.03-aarch64")
            script_args=("openEuler2203_aarch64" "$target_dir")
            ;;
        "openEuler24.03-x86_64")
            script_args=("openEuler2403_x86_64" "$target_dir")
            ;;
        "openEuler24.03-aarch64")
            script_args=("openEuler2403_aarch64" "$target_dir")
            ;;
        *)
            echo "Error: Invalid platform parameter '$platform'" >&2
            exit 1;
            ;;
    esac

    if ! sh main.sh "${script_args[@]}"; then
        echo "Error: Failed to download packages" >&2
        exit 1;
    fi

    echo "Download the RPM packages successfully"

    if ! cd - >/dev/null; then
        echo "Warning: Failed to return to original directory" >&2
    fi
}

package_portal() {
    echo "Start to package the portal"
    mvn clean package -DskipTests
    echo "Package the portal successfully"
}

build_dirs() {
    echo "Start to build the directories"
    cd portal
    chmod +x ./bin/*
    cp ../target/openGauss-portal-*.jar ./

    mkdir -p pkg/chameleon pkg/confluent pkg/datachecker pkg/debezium pkg/full-migration
    mkdir -p template/config/chameleon template/config/datachecker template/config/debezium template/config/full-migration
    echo "Build the directories successfully"
}

check_param $@
config_properties $@
download_dependencies $@
package_portal
build_dirs

# Next, copy the migration tools installation packages and configuration files to the specified directories, and package the entire portal directory as a tar.gz file, complete the packaging.