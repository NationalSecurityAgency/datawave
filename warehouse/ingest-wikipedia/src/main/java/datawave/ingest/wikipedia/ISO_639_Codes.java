package datawave.ingest.wikipedia;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

/**
 * 
 */
public class ISO_639_Codes {
    public static final Multimap<String,String> ISO_639_1 = ImmutableMultimap.<String,String> builder().put("aa", "AFAR").put("ab", "ABKHAZIAN")
                    .put("ae", "AVESTAN").put("af", "AFRIKAANS").put("ak", "AKAN").put("am", "AMHARIC").put("an", "ARAGONESE").put("ar", "ARABIC")
                    .put("as", "ASSAMESE").put("av", "AVARIC").put("ay", "AYMARA").put("az", "AZERBAIJANI").put("ba", "BASHKIR").put("be", "BELARUSIAN")
                    .put("bg", "BULGARIAN").put("bh", "BIHARI").put("bi", "BISLAMA").put("bm", "BAMBARA").put("bn", "BENGALI").put("bo", "TIBETAN")
                    .put("br", "BRETON").put("bs", "BOSNIAN").putAll("ca", "CATALAN", "VALENCIAN").put("ce", "CHECHEN").put("ch", "CHAMORRO")
                    .put("co", "CORSICAN").put("cr", "CREE").put("cs", "CZECH").put("cv", "CHUVASH").put("cy", "WELSH").put("da", "DANISH").put("de", "GERMAN")
                    .putAll("dv", "DIVEHI", "DHIVEHI", "MALDIVIAN").put("dz", "DZONGKHA").put("ee", "EWE").putAll("el", "GREEK, MODERN (1453-)")
                    .put("en", "ENGLISH").put("eo", "ESPERANTO").putAll("es", "SPANISH", "CASTILIAN").put("et", "ESTONIAN").put("eu", "BASQUE")
                    .put("fa", "PERSIAN").put("ff", "FULAH").put("fi", "FINNISH").put("fj", "FIJIAN").put("fo", "FAROESE").put("fr", "FRENCH")
                    .put("fy", "WESTERN FRISIAN").put("ga", "IRISH").putAll("gd", "GAELIC", "SCOTTISH GAELIC").put("gl", "GALICIAN").put("gn", "GUARANI")
                    .put("gu", "GUJARATI").put("gv", "MANX").put("ha", "HAUSA").put("he", "HEBREW").put("hi", "HINDI").put("ho", "HIRI MOTU")
                    .put("hr", "CROATIAN").putAll("ht", "HAITIAN", "HAITIAN CREOLE").put("hu", "HUNGARIAN").put("hy", "ARMENIAN").put("hz", "HERERO")
                    .put("ia", "INTERLINGUA (INTERNATIONAL AUXILIARY LANGUAGE ASSOCIATION)").put("id", "INDONESIAN").put("ie", "INTERLINGUE").put("ig", "IGBO")
                    .put("ii", "SICHUAN YI").put("ik", "INUPIAQ").put("io", "IDO").put("is", "ICELANDIC").put("it", "ITALIAN").put("iu", "INUKTITUT")
                    .put("ja", "JAPANESE").put("jv", "JAVANESE").put("ka", "GEORGIAN").put("kg", "KONGO").putAll("ki", "KIKUYU", "GIKUYU")
                    .putAll("kj", "KUANYAMA", "KWANYAMA").put("kk", "KAZAKH").putAll("kl", "KALAALLISUT", "GREENLANDIC").put("km", "CENTRAL KHMER")
                    .put("kn", "KANNADA").put("ko", "KOREAN").put("kr", "KANURI").put("ks", "KASHMIRI").put("ku", "KURDISH").put("kv", "KOMI")
                    .put("kw", "CORNISH").putAll("ky", "KIRGHIZ", "KYRGYZ").put("la", "LATIN").putAll("lb", "LUXEMBOURGISH", "LETZEBURGESCH").put("lg", "GANDA")
                    .putAll("li", "LIMBURGAN", "LIMBURGER", "LIMBURGISH").put("ln", "LINGALA").put("lo", "LAO").put("lt", "LITHUANIAN")
                    .put("lu", "LUBA-KATANGA").put("lv", "LATVIAN").put("mg", "MALAGASY").put("mh", "MARSHALLESE").put("mi", "MAORI").put("mk", "MACEDONIAN")
                    .put("ml", "MALAYALAM").put("mn", "MONGOLIAN").put("mo", "MOLDAVIAN").put("mr", "MARATHI").put("ms", "MALAY").put("mt", "MALTESE")
                    .put("my", "BURMESE").put("na", "NAURU").putAll("nb", "BOKMÅL, NORWEGIAN", "NORWEGIAN BOKMÅL")
                    .putAll("nd", "NDEBELE, NORTH", "NORTH NDEBELE").put("ne", "NEPALI").put("ng", "NDONGA").putAll("nl", "DUTCH", "FLEMISH")
                    .putAll("nn", "NORWEGIAN NYNORSK", "NYNORSK, NORWEGIAN").put("no", "NORWEGIAN").putAll("nr", "NDEBELE, SOUTH", "SOUTH NDEBELE")
                    .putAll("nv", "NAVAJO", "NAVAHO").putAll("ny", "CHICHEWA", "CHEWA", "NYANJA").put("oj", "OJIBWA").put("om", "OROMO").put("or", "ORIYA")
                    .putAll("os", "OSSETIAN", "OSSETIC").putAll("pa", "PANJABI", "PUNJABI").put("pi", "PALI").put("pl", "POLISH").put("ps", "PASHTO")
                    .put("pt", "PORTUGUESE").put("qu", "QUECHUA").put("rm", "ROMANSH").put("rn", "RUNDI").put("ro", "ROMANIAN").put("ru", "RUSSIAN")
                    .put("rw", "KINYARWANDA").put("sa", "SANSKRIT").put("sc", "SARDINIAN").put("sd", "SINDHI").put("se", "NORTHERN SAMI").put("sg", "SANGO")
                    .putAll("si", "SINHALA", "SINHALESE").put("sk", "SLOVAK").put("sl", "SLOVENIAN").put("sm", "SAMOAN").put("sn", "SHONA").put("so", "SOMALI")
                    .put("sq", "ALBANIAN").put("sr", "SERBIAN").put("ss", "SWATI").putAll("st", "SOTHO, SOUTHERN").put("su", "SUNDANESE").put("sv", "SWEDISH")
                    .put("sw", "SWAHILI").put("ta", "TAMIL").put("te", "TELUGU").put("tg", "TAJIK").put("th", "THAI").put("ti", "TIGRINYA").put("tk", "TURKMEN")
                    .put("tl", "TAGALOG").put("tn", "TSWANA").put("to", "TONGA (TONGA ISLANDS)").put("tr", "TURKISH").put("ts", "TSONGA").put("tt", "TATAR")
                    .put("tw", "TWI").put("ty", "TAHITIAN").putAll("ug", "UIGHUR", "UYGHUR").put("uk", "UKRAINIAN").put("ur", "URDU").put("uz", "UZBEK")
                    .put("ve", "VENDA").put("vi", "VIETNAMESE").put("vo", "VOLAPÜK").put("wa", "WALLOON").put("wo", "WOLOF").put("xh", "XHOSA")
                    .put("yi", "YIDDISH").put("yo", "YORUBA").putAll("za", "ZHUANG", "CHUANG").put("zh", "CHINESE").put("zu", "ZULU").build();
}
