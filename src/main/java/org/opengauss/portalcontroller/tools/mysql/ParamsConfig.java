package org.opengauss.portalcontroller.tools.mysql;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.portalcontroller.utils.PropertitesUtils;
import org.opengauss.portalcontroller.utils.YmlUtils;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import static org.opengauss.portalcontroller.PortalControl.toolsConfigParametersTable;

/**
 * ParamsConfig
 *
 * @date :2023/11/3 15:22
 * @description: ParamsConfig
 * @version: 1.1
 * @since 1.1
 */
@Slf4j
public abstract class ParamsConfig {
    // configName:configParmasMap
    Map<String, Map<String, Object>> configYmlChangeParamsMap;

    // configName:configParmasMap
    Map<String, Map<String, Object>> configPropsChangeParamsMap;

    // configName:configParams
    Map<String, List<String>> configDeleteParamsMap;

    public ParamsConfig() {
        this.configYmlChangeParamsMap = new HashMap<>();
        this.configPropsChangeParamsMap = new HashMap<>();
        this.configDeleteParamsMap = new HashMap<>();
    }

    void changeAllConfig() {
        for (Map.Entry<String, Map<String, Object>> configChangeParams
                : this.configYmlChangeParamsMap.entrySet()) {
            Map<String, Object> changeParamMap = configChangeParams.getValue();
            if (changeParamMap.isEmpty()) {
                continue;
            }
            log.info("path:{} start change...",
                    toolsConfigParametersTable.get(configChangeParams.getKey()));
            YmlUtils.changeYmlParameters(changeParamMap, toolsConfigParametersTable.get(configChangeParams.getKey()));
        }
        for (Map.Entry<String, Map<String, Object>> configChangeParams
                : this.configPropsChangeParamsMap.entrySet()) {
            Map<String, Object> changeParamMap = configChangeParams.getValue();
            if (changeParamMap.isEmpty()) {
                continue;
            }
            Hashtable<String, String> hashtable = new Hashtable<>();
            for (Map.Entry<String, Object> param : changeParamMap.entrySet()) {
                hashtable.put(param.getKey(), String.valueOf(param.getValue()));
            }
            log.info("path:{} start change...",
                    toolsConfigParametersTable.get(configChangeParams.getKey()));
            PropertitesUtils.changePropertiesParameters(hashtable,
                    toolsConfigParametersTable.get(configChangeParams.getKey()));
        }
    }

    void deleteParamsConifg() {
        for (Map.Entry<String, List<String>> configDeleteParams : configDeleteParamsMap.entrySet()) {
            String configPath = configDeleteParams.getKey();
            List<String> deleteParams = configDeleteParams.getValue();
            if (deleteParams.isEmpty()) {
                continue;
            }
            if (configPath.endsWith("yml")) {
                YmlUtils.deleteYmlParameters(deleteParams, toolsConfigParametersTable.get(configPath));
            } else {
                PropertitesUtils.deletePropParameters(deleteParams, toolsConfigParametersTable.get(configPath));
            }
        }
    }

    abstract void initConfigChangeParamsMap();

    abstract void initDataBaseParams();

    abstract void initWorkSpaceParams(String workspaceId);

    abstract void initInteractionParams();

    abstract void initParmasFromEnvForAddAndChange();

    abstract void initParmasFromEnvForDelete();

    abstract void initKafkaParams();

    void setAllParams(String workSpaceId) {
        initDataBaseParams();
        initWorkSpaceParams(workSpaceId);
        initInteractionParams();
        initParmasFromEnvForAddAndChange();
        initParmasFromEnvForDelete();
        initKafkaParams();
    }
}
