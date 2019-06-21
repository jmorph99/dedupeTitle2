/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author john-lt
 */

package Classify;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;




public class DedupeMain {
    public static final WhitespaceAnalyzer sa = new WhitespaceAnalyzer();
    private static String user;
    private static String pass;
    private static int count = 0;
    public static void main(String[] args)
    throws Exception {
 String post = "{\n" +
"  \"size\": 0,\n" +
"  \"query\": {\n" +
"    \"bool\": {\n" +
"      \"must_not\": [\n" +
"        {\n" +
"          \"match\": {\n" +
"            \"isduplicate\": true\n" +
"          }\n" +
"        }\n" +
"      ]\n" +
"    }\n" +
"  },\n" +
"  \"aggs\": {\n" +
"    \"similars\": {\n" +
"      \"terms\": {\n" +
"        \"field\": \"pname.keyword\",\n" +
"        \"size\": 500,\n" +
"        \"min_doc_count\": 2\n" +
"      }\n" +
"    }\n" +
"  }\n" +
"}";
        String ip = args[0];
        String index = args[1];
        DedupeMain.user = args[2];
        DedupeMain.pass = args[3];
        if(!ip.endsWith("/"))
            ip += "/";
        if(!index.endsWith("/"))
            index += "/";
        long starttime = System.currentTimeMillis();
        JSONObject root = loadLifeFromElastic.loadDuplicates(ip + index + "_search",post);
        
        JSONObject aggs =  (JSONObject)root.get("aggregations");
        JSONObject similars =  (JSONObject) aggs.get("similars");
        JSONArray buckets =  (JSONArray) similars.get("buckets");
        while(buckets.size() > 0)
        {
            ThreadPoolExecutor exService = new ThreadPoolExecutor(8, 8, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(1000));
            
            for(int i = 0; i < buckets.size()-1; i++)
            {

                Runnable worker;
                worker = new dedupeThread((JSONObject)buckets.get(i), ip , index);
                exService.execute(worker);
                ;
                while (exService.getQueue().size() > 50)
                    try {
                      Thread.sleep(1000L);
                    } catch (InterruptedException ex) {
                      Logger.getLogger(DedupeMain.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }


                exService.shutdown();
                while(!exService.isTerminated())
                    Thread.sleep(5000);
                root = loadLifeFromElastic.loadDuplicates(ip + index + "_search",post);
            //System.out.println(root);
                aggs =  (JSONObject)root.get("aggregations");
                similars =  (JSONObject) aggs.get("similars");
                buckets =  (JSONArray) similars.get("buckets");
        }
    }
    
    static void hsAdd(HashSet<String> hs, String str){
        if(!hs.contains(str))
            hs.add(str);
    }

    public static String getimgURL(JSONObject json){
       
                
        JSONObject duplicateFields = (JSONObject) json.get("fields");
        JSONArray rawPNameArray = (JSONArray) duplicateFields.get("imageURL.imgURL");
        return (String) rawPNameArray.get(0);

    }
       
    public static class loadLifeFromElastic{
        public static JSONObject loadDuplicates(String surl, String req) throws ProtocolException, MalformedURLException, IOException, ParseException{
            byte[] postData       = req.getBytes( StandardCharsets.UTF_8 );
            int    postDataLength = postData.length;
            URL    url            = new URL( surl );
            Authenticator.setDefault (new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication (DedupeMain.user, DedupeMain.pass.toCharArray());
                }
            });
            HttpURLConnection conn= (HttpURLConnection) url.openConnection();           
            conn.setDoOutput( true );
            conn.setInstanceFollowRedirects( false );
            conn.setRequestMethod( "POST" );
            conn.setRequestProperty( "Content-Type", "application/json"); 
            conn.setRequestProperty( "charset", "utf-8");
            conn.setRequestProperty( "Content-Length", Integer.toString( postDataLength ));
            conn.setUseCaches( false );
            try( DataOutputStream wr = new DataOutputStream( conn.getOutputStream())) {
                wr.write(postData );
                wr.flush();
                wr.close();
            }
            System.out.println(++count);
            BufferedReader in = new BufferedReader(
		        new InputStreamReader(conn.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		
		//print result
                String output = response.toString();
		//System.out.println(output);
                     
            JSONParser parser = new JSONParser();
            //System.out.println();
            JSONObject jsonObject = (JSONObject) parser.parse(output);
            return jsonObject;
            //System.out.println(conn.getResponseMessage());
        }
    }
    
    public synchronized static ArrayList<String> chopStrings(String str){
        ArrayList<String> bits = new ArrayList<>();
        try {
            
            TokenStream ts = sa.tokenStream("text", str.toLowerCase());
            CharTermAttribute charTermAttribute = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            
            while(ts.incrementToken()){
                
                String slice = charTermAttribute.toString();
                bits.add(slice);
                //System.out.println(slice);
                        }
            ts.close();
        } catch (IOException ex) {
            Logger.getLogger(DedupeMain.class.getName()).log(Level.SEVERE, null, ex);
        }
        return bits;
    }
    

}
