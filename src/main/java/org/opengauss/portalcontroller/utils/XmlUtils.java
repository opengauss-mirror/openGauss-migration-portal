/*
 *
 *  * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *  *
 *  * openGauss is licensed under Mulan PSL v2.
 *  * You can use this software according to the terms and conditions of the Mulan PSL v2.
 *  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 *  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 *  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 *  * See the Mulan PSL v2 for more details.
 *
 */

package org.opengauss.portalcontroller.utils;

import lombok.extern.slf4j.Slf4j;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * XML Utils
 *
 * @author: www
 * @date: 2023/11/28 11:14
 * @description: msg
 * @since: 1.1
 * @version: 1.1
 */
@Slf4j
public class XmlUtils {
    /**
     * load TheXMLFile
     *
     * @param path path
     * @return Document Document
     */
    public static Optional<Document> loadXml(String path) {
        SAXBuilder saxBuilder = new SAXBuilder();
        saxBuilder.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        saxBuilder.setFeature("http://xml.org/sax/features/external-general-entities", false);
        saxBuilder.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        saxBuilder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        try {
            FileInputStream fis = new FileInputStream(path);
            return Optional.of(saxBuilder.build(fis));
        } catch (JDOMException | IOException e) {
            log.error("loadXml failed ... ", e);
        }
        return Optional.empty();
    }

    /**
     * Obtain the attribute information of the log4j2.xml file
     *
     * @param name name
     * @param doc  doc
     * @return String
     */
    public static Optional<String> getLog4j2Properties(String name, Document doc) {
        if (doc == null) {
            log.error("get datacheck log doc is null...");
            return Optional.empty();
        }
        Element child = doc.getRootElement().getChild("Properties");
        List<Element> elements = child.getChildren("Property");
        for (Element element : elements) {
            String attrName = element.getAttributeValue(name);
            if (Objects.equals(attrName, "LOG_LEVEL")) {
                return Optional.of(element.getValue());
            }
        }
        return Optional.empty();
    }
}
