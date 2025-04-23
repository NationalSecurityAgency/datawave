package datawave.microservice.dictionary.config;

import datawave.webservice.dictionary.data.DataDictionaryBase;
import datawave.webservice.dictionary.data.DescriptionBase;
import datawave.webservice.dictionary.data.DictionaryFieldBase;
import datawave.webservice.dictionary.data.FieldsBase;
import datawave.webservice.metadata.MetadataFieldBase;

public interface ResponseObjectFactory<DESC extends DescriptionBase<DESC>,DICT extends DataDictionaryBase<DICT,META>,META extends MetadataFieldBase<META,DESC>,FIELD extends DictionaryFieldBase<FIELD,DESC>,FIELDS extends FieldsBase<FIELDS,FIELD,DESC>> {
    DICT getDataDictionary();
    
    DESC getDescription();
    
    FIELDS getFields();
}
