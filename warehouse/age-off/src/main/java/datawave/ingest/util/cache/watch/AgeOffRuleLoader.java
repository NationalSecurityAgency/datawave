package datawave.ingest.util.cache.watch;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.Logger;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.ageoff.FilterOptions;
import datawave.iterators.filter.ageoff.FilterRule;

public class AgeOffRuleLoader {
    private static final Logger log = Logger.getLogger(AgeOffRuleLoader.class);
    private final IteratorEnvironment iterEnv;
    private final XmlRuleProcessor xmlRuleProcessor;

    public AgeOffRuleLoader(AgeOffFileLoaderDependencyProvider loaderConfig, IteratorEnvironment iterEnv) {
        this.iterEnv = iterEnv;
        this.xmlRuleProcessor = new XmlRuleProcessor(loaderConfig);
    }

    public List<FilterRule> load(InputStream in) throws IOException {
        List<RuleConfig> mergedRuleConfigs = xmlRuleProcessor.loadRuleConfigs(in);
        List<FilterRule> filterRules = new ArrayList<>();

        /*
         * This has been changed to support extended options.
         */
        for (RuleConfig ruleConfig : mergedRuleConfigs) {
            try {
                if (ruleConfig.getFilterClassName() == null) {
                    throw new IllegalArgumentException("The filter class must not be null");
                }

                FilterRule filter = (FilterRule) Class.forName(ruleConfig.getFilterClassName()).getDeclaredConstructor().newInstance();

                FilterOptions option = new FilterOptions();

                if (ruleConfig.getTtlValue() != null) {
                    option.setTTL(Long.parseLong(ruleConfig.getTtlValue()));
                }
                if (ruleConfig.getTtlUnits() != null) {
                    option.setTTLUnits(ruleConfig.getTtlUnits());
                }
                option.setOption(AgeOffConfigParams.MATCHPATTERN, ruleConfig.getMatchPattern());

                StringBuilder extOptions = new StringBuilder();

                for (Map.Entry<String,String> myOption : ruleConfig.getExtendedOptions().entrySet()) {
                    option.setOption(myOption.getKey(), myOption.getValue());
                    extOptions.append(myOption.getKey()).append(",");
                }

                int extOptionLen = extOptions.length();

                option.setOption(AgeOffConfigParams.EXTENDED_OPTIONS, extOptions.substring(0, extOptionLen - 1));

                filter.init(option, iterEnv);

                filterRules.add(filter);

            } catch (IllegalArgumentException | InstantiationException | ClassNotFoundException | IllegalAccessException | InvocationTargetException
                            | NoSuchMethodException e) {
                log.trace("An error occurred while loading age-off rules, the exception will be rethrown", e);
                throw new IOException(e);
            }
        }
        return filterRules;
    }
}
