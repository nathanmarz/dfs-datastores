/**
 * 
 */
package com.backtype.hadoop.pail;

/**
 * @author Derrick Burns <derrick.burns@rincaro.com>
 *
 */

import cascading.bind.json.FieldsSerializer;

import com.fasterxml.jackson.databind.module.SimpleModule;

public class CascadingModule extends SimpleModule {
    private static final long serialVersionUID = 1L;

    public CascadingModule() {
        super(ModuleVersion.instance.version());
        addSerializer(new FieldsSerializer());

    }

}
