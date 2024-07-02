package datawave.query.language.analyzers.lucene;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CJKLuceneAnalyzerTest extends BaseLuceneTest {

    @BeforeEach
    public void setup() {
        analyzer = new CJKLuceneAnalyzer();
    }

    @Test
    public void testChinesePhrase() {
        // the bravest tank man
        // Zuì yǒnggǎn de tǎnkè rén
        // 最勇敢的坦克人
        inputs.put("最勇敢的坦克人", Set.of("最勇", "勇敢", "敢的", "的坦", "坦克", "克人"));
        test(inputs);
    }

    @Test
    public void testJapanesePhrase() {
        // samurai or ninja
        // Samurai toka ninja toka
        // サムライとか忍者とか
        inputs.put("サムライとか忍者とか", Set.of("サム", "ムラ", "ライ", "イと", "とか", "か忍", "忍者", "者と"));
        test(inputs);

        // welcome to night city
        // Yoru no machi e yōkoso
        // 夜の街へようこそ
        inputs.put("夜の街へようこそ", Set.of("夜の", "の街", "街へ", "へよ", "よう", "うこ", "こそ"));
        test(inputs);
    }

    @Test
    public void testKoreanPhrase() {
        // our grandparents watch k-dramas
        // uli jobumonim-eun hangug deulamaleul bwayo
        // 우리 조부모님은 한국 드라마를 봐요
        inputs.put("우리", Set.of());
        inputs.put("조부모님은", Set.of("조부", "부모", "모님", "님은"));
        inputs.put("한국", Set.of());
        inputs.put("드라마를", Set.of("드라", "라마", "마를"));
        inputs.put("봐요", Set.of());
        test(inputs);
    }
}
