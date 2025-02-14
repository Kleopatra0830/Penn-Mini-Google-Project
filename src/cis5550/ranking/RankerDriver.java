package cis5550.ranking;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import cis5550.model.UrlRank;
import cis5550.webserver.Server;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RankerDriver {
    private static final Logger logger = Logger.getLogger(RankerDriver.class);

    public static void main(String[] args) throws IOException {
        //args: args[0]: port; args[1]: kvs address
        if(args.length != 2) {
            throw new IllegalArgumentException("args: args[0]: port; args[1]: kvs address");
        }

//        Ranker ranker = new Ranker(args[0], "aac");
//        List<UrlRank> result = ranker.rank();
//        System.out.println(result);

        String kvsAddr = args[1];
        KVSClient client = new KVSClient(kvsAddr);
        List<String> wordList = new ArrayList<>();
        System.out.println("Started\nLoading word list...");
        Iterator<Row> iterator = client.scan("pt-index");
        while (iterator.hasNext()) {
            wordList.add(iterator.next().key());
        }
        System.out.println("Word list loaded " + wordList.size() + " words");

        Server.port(Integer.parseInt(args[0]));
        Server.staticFiles.location("./frontend");
        Server.get("/", ((request, response) -> {
            String filePath = "./frontend/SearchPage.html";
            System.out.println("Home page route");
            StringBuilder htmlBuilder = new StringBuilder();

            try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    htmlBuilder.append(line).append("\n");
                }
            } catch (IOException e) {
                logger.error("Error reading file " + filePath, e);
            }
            response.type("text/html");
            return htmlBuilder.toString();
        }));

        Server.get("/search", ((request, response) -> {
            System.out.println("Search route");
            String query = URLDecoder.decode(request.queryParams("query"), StandardCharsets.UTF_8);
            logger.debug("query: " + query);
            JSONArray array = new JSONArray();
            Ranker ranker = new Ranker(args[1], query);
            List<UrlRank> result = ranker.rank();
            System.out.println(result);
            for (UrlRank urlRank : result) {
                JSONObject obj = new JSONObject();
                obj.put("url", urlRank.getUrl());
                byte[] pageBytes = client.get("pt-crawl", Hasher.hash(urlRank.getUrl()), "page");
                Document doc = Jsoup.parse(new String(pageBytes));
                obj.put("page", doc.text().substring(0, 100) + "...");
                obj.put("title", doc.title());
                array.put(obj);
            }

            return array.toString();
        }));

        Server.get("/wordlist", (req, res) -> {
            JSONArray jsonArray = new JSONArray();
            jsonArray.put(wordList);
            return jsonArray.toString();
        });
    }
}
