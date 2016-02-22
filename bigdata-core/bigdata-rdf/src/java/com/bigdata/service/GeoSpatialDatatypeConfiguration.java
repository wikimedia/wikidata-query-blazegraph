/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Feb 10, 2016
 */
package com.bigdata.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.bigdata.service.GeoSpatialDatatypeFieldConfiguration.ServiceMapping;

/**
 * Configuration of a single geospatial datatype, including value type, multiplier,
 * min possible value, and mapping to the service.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialDatatypeConfiguration {
    
    final static private Logger log = Logger.getLogger(GeoSpatialDatatypeConfiguration.class);

    // URI of the datatype, e.g. <http://my.custom.geospatial.coordinate>
    private final String uri;

    // ordered list of fields defining the datatype
    private List<GeoSpatialDatatypeFieldConfiguration> fields;

    /**
     * Constructor, setting up a {@link GeoSpatialDatatypeConfiguration} given a uri and a
     * JSON array defining the fields as input. Throws an {@link IllegalArgumentException}
     * if the uri is null or empty or in case the JSON array does not describe a set of
     * valid fields.
     */
    public GeoSpatialDatatypeConfiguration(String uri, JSONArray fieldsJson) {
        
        if (uri==null || uri.isEmpty())
            throw new IllegalArgumentException("URI parameter must not be null or empty");
        
        this.uri = uri;
        
        fields = new ArrayList<GeoSpatialDatatypeFieldConfiguration>();
        
        /**
         * We expect a JSON array of the following format (example):
         * 
         * [ 
         *   { "valueType": "DOUBLE", "multiplier": "100000", "serviceMapping": "LATITUDE" }, 
         *   { "valueType": "DOUBLE", "multiplier": "100000", "serviceMapping": "LONGITUDE" }, 
         *   { "valueType": "LONG, "multiplier": "1", "minValue" : "0" , "serviceMapping": "TIME"  }, 
         *   { "valueType": "LONG", "multiplier": "1", "minValue" : "0" , "serviceMapping" : "COORD_SYSTEM"  } 
         * ]
         */
        for (int i=0; i<fieldsJson.length(); i++) {

            try {
                
                // { "valueType": "double", "multiplier": "100000", "serviceMapping": "latitude" }
                JSONObject fieldJson = (JSONObject)fieldsJson.getJSONObject(i);
                
                fields.add(new GeoSpatialDatatypeFieldConfiguration(fieldJson));

            } catch (JSONException e) {
                
                log.warn("Invalid JSON for field description: " + e.getMessage());
                throw new IllegalArgumentException(e); // forward exception
            }
        }
        
        // validate that there is at least one field defined for the geospatial datatype
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(
                "Geospatial datatype config for datatype " + uri + " must have at least one field, but has none.");
        }
        
        // validate that there are no duplicate service mappings used for the fields
        final Set<ServiceMapping> serviceMappings = new HashSet<ServiceMapping>();
        for (int i=0; i<fields.size(); i++) {
            
            final ServiceMapping curServiceMapping = fields.get(i).getServiceMapping();
            
            if (serviceMappings.contains(curServiceMapping)) {
                throw new IllegalArgumentException("Duplicate URI used for geospatial datatype config: " + curServiceMapping);
            }
            
            serviceMappings.add(curServiceMapping);
        }
        
    }
    
    public String getUri() {
        return uri;
    }
    
    /**
     * Returns the list of fields defining the datatype. Never null.
     */
    public List<GeoSpatialDatatypeFieldConfiguration> getFields() {
        return fields;
    }

}