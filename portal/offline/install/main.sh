#!/bin/bash

# 脚本使用说明
usage=""

# portal jar文件地址
portal_control_path=""

# 安装包下载地址
download_urls=""

# portal安装所需rpm包
portal_packages=""

# chameleon安装所需rpm包
chameleon_packages=""

# 有效的系统架构
valid_system_archs=("CentOS7_x86_64" "openEuler2003_x86_64" "openEuler2003_aarch64" "openEuler2203_x86_64" "openEuler2203_aarch64")

generate_usage() {
    temp=""

    for ((i=0; i<${#valid_system_archs[@]}; i++))
    do
        if [ $i -eq 0 ]; then
            temp="${valid_system_archs[i]}"
        else
            temp="${temp}|${valid_system_archs[i]}"
        fi
    done

    usage="Usage: $0 <${temp}> <portal_control_path>"
}

check_path() {
    portal_control_path=$2

    # 检查路径是否存在
    if [ -d "${portal_control_path}" ]; then
        # 检查是否有写权限
        if [ ! -w "${portal_control_path}" ]; then
            echo "You do not have write permission on directory '${portal_control_path}'."
            exit 1
        fi
    else
        echo "Directory '${portal_control_path}' does not exist. Creating it now..."

        # 创建目录及其父目录（如果不存在）
        if mkdir -p "${portal_control_path}"; then
            echo "Directory '${portal_control_path}' created successfully."
        else
            echo "Failed to create directory '${portal_control_path}'."
            exit 1
        fi
    fi
}

check_parameters() {
    # 检查参数数量是否为2
    if [ "$#" -ne 2 ]; then
        echo "Required two parameters."
        echo "${usage}"
        exit 1
    fi

    # 检查系统及架构是否匹配
    if [[ ! " ${valid_system_archs[@]} " =~ " $1 " ]]; then
        echo "The first parameter is invalid."
        echo "${usage}"
        exit 1
    fi

    # 检查portal jar文件路径是否可用
    check_path $@
}

read_properties() {
    echo "Read the properties file."

    # 指定要读取的properties文件
    properties_file="./download_urls_packages.properties"

    # 判断properties文件是否存在
    if [ ! -f "${properties_file}" ]; then
        echo "File '${properties_file}' does not exist."
        exit 1;
    fi

    # 逐行读取并解析.properties文件
    while IFS='=' read -r key value; do
        # 忽略注释和空行
        if [[ ${key} != "" && ! ${key} == \#* ]]; then
            case ${key} in
                "$1_url")
                    download_urls=${value}
                    ;;
                "$1_pkg_portal")
                    portal_packages=${value}
                    ;;
                "$1_pkg_chameleon")
                    chameleon_packages=${value}
                    ;;
            esac
        fi
    done < "${properties_file}"
}

write_shell() {
    dependencies_path="${portal_control_path}/pkg/dependencies/"
    mkdir -p "${dependencies_path}" && touch "${dependencies_path}/download_dependencies.sh"

    # 输出下载rpm包的脚本
    echo "Generating download_dependencies.sh."
    cat << EOF > "${dependencies_path}/download_dependencies.sh"
#!/bin/bash

# portal jar文件路径
portal_control_path="${portal_control_path}"

# rpm包下载地址的数组
download_urls=(${download_urls})

# rpm包保存路径
rpms_path="\${portal_control_path}/pkg/dependencies/rpms"

# 循环遍历下载rpm包
for url in "\${download_urls[@]}"
do
    echo "\$url"
    wget -q -P \${rpms_path} \${url}
    if [ \$? -eq 0 ]; then
        echo "download success!"
    else
        echo "download failed!"
    fi
done

echo "shell end"
EOF
    echo "Generated download_dependencies.sh successfully."

    # 输出安装rpm包的脚本
    echo "Generating install_dependencies.sh."
    cat << EOF > "${dependencies_path}/install_dependencies.sh"
#!/bin/bash

# portal依赖的rpm包
portal_packages=(${portal_packages})

# chameleon依赖的rpm包
chameleon_packages=(${chameleon_packages})

# 需要安装的rpm包
packages=("\${portal_packages[@]}")

# 安装依赖的方法
install() {
    for package in "\${packages[@]}"
    do
        echo ""
        echo "\${package}"
        output=\$(sudo rpm -Uvh ./rpms/\${package} --nodeps --force)
        if [ \$? -eq 0 ] || echo "\${output}" | grep -q "already installed"; then
            echo "Install success!"
        else
            echo "\${output}"
            echo "\${package} install failed!"
        fi
    done
}

case \$1 in
    "portal")
        echo "Start install the portal dependencies."
        install
        ;;
    "chameleon")
        echo "Start install the chameleon dependencies."
        packages=("\${chameleon_packages[@]}")
        install
        ;;
    *)
         echo "Usage: \$0 {portal|chameleon}"
        ;;
esac

echo "shell end"
EOF
    echo "Generated install_dependencies.sh successfully."

    # 添加执行权限
    chmod +x "${dependencies_path}/download_dependencies.sh"
    chmod +x "${dependencies_path}/install_dependencies.sh"
}

download_rpms() {
    echo "Start to download the RPM packages."
    source ${dependencies_path}/download_dependencies.sh
}

main() {
    generate_usage
    check_parameters $@
    read_properties $@
    write_shell
    download_rpms
    echo "main shell end"
}

main $@