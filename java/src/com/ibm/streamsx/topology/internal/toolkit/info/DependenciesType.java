//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.8-b130911.1802 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.09.12 at 01:06:02 PM PDT 
//


package com.ibm.streamsx.topology.internal.toolkit.info;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for dependenciesType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="dependenciesType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="toolkit" type="{http://www.ibm.com/xmlns/prod/streams/spl/common}toolkitDependencyType" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "dependenciesType", propOrder = {
    "toolkit"
})
public class DependenciesType {

    protected List<ToolkitDependencyType> toolkit;

    /**
     * Gets the value of the toolkit property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the toolkit property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getToolkit().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link ToolkitDependencyType }
     * 
     * 
     */
    public List<ToolkitDependencyType> getToolkit() {
        if (toolkit == null) {
            toolkit = new ArrayList<ToolkitDependencyType>();
        }
        return this.toolkit;
    }

}
