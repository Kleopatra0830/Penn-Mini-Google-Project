package cis5550.tools;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

//TODO: Accurate test for processHtmlString
public class HtmlProcessor {

    /**
     * Extracts distinct words from html string
     * @param html html content as a string
     * @return list of distinct words
     */
    public static Map<String, Integer> extractWords(String html) {
        String noHtml = html.replaceAll("<[^>]*>", "");
        String noPunctuation = noHtml.replaceAll("\\p{Punct}", "");
        String noWhitespace = noPunctuation.replaceAll("\\s+", " ");
        String lowerCase = noWhitespace.toLowerCase();
        return Arrays.stream(lowerCase.split("\\s+"))
                .filter(word -> !word.isEmpty())
                .collect(Collectors.toMap(word -> word, word -> 1, Integer::sum));
    }

    public static void main(String[] args) {
        String htmlString = "<apple> apple apple banana banana <pear>";
        var processedString = extractWords(htmlString);
        System.out.println(processedString);
    }
}
