/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Classify;

import static Classify.DedupeMain.chopStrings;
import static Classify.DedupeMain.hsAdd;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author john-lt
 */
public class dedupeThread implements Runnable
{
        private JSONObject key;
        private String ip;
        private String index;
        

    public dedupeThread(JSONObject tkey, String tip, String tindex)
    {
        key = tkey;
        ip = tip;
        index = tindex;
    }
    @Override
    public void run() {
        
             
        StringBuilder msb = new StringBuilder();
        HashSet<String> hs = new HashSet<>(); 
        String tkey = (String)key.get("key");
        ArrayList<JSONObject> dupes = new ArrayList<>();
        String post = "{\n" +
"  \n" +
"  \"size\": 5000,\n" +
"  \"docvalue_fields\": [\n" +
"    \"pname.keyword\",\n" +
"    \"searchPname.keyword\",\n" +
"  ],\n" +
"  \"_source\": \"\",\n" +
"  \"query\": {\n" +
"    \"bool\": {\n" +
"      \"must\": [\n" +
"        {\n" +
"                   \"match\": {\"pname.keyword\":\"" + tkey.replace("\"", "\\\"") + "\"}\n" +
"        }\n" +
"      ],\n" +
"      \"must_not\": [\n" +
"        {\"match\": {\n" +
"          \"isduplicate\": true\n" +
"        }}\n" +
"      ]\n" +
"    }\n" +
"  },\n" +
"  \"sort\": [\n" +
"    {\n" +
"      \"pname.keyword\": {\n" +
"        \"order\": \"asc\"\n" +
"      }\n" +
"    }\n" +
"  ]\n" +
"}";
        JSONObject root = null;;
		try {
			root = DedupeMain.loadLifeFromElastic.loadDuplicates(ip + index + "_search?scroll=5m",post);
		} catch (ProtocolException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        JSONArray hits = ((JSONArray)((JSONObject)root.get("hits")).get("hits"));
        JSONObject master = (JSONObject)hits.get(0);
        String scrollid = (String) root.get("_scroll_id");
        for(int i=1;i<hits.size();i++)
            dupes.add((JSONObject)hits.get(i));
        String scrollCommand = "{\n" +
"  \"scroll\":\"5m\",\n" +
"  \"scroll_id\":\"$SCROLL\"}}";
        
        while(hits.size()>0){
            try {
				root = DedupeMain.loadLifeFromElastic.loadDuplicates(ip + "_search/scroll",scrollCommand.replace("$SCROLL", scrollid));
			} catch (ProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            scrollid = (String) root.get("_scroll_id");
            hits = ((JSONArray)((JSONObject)root.get("hits")).get("hits"));
            for(int i=0;i<hits.size();i++)
                dupes.add((JSONObject)hits.get(i));
        }
        String masterId = (String) master.get("_id");
        String masterIndex = (String) master.get("_index");
        
        JSONObject masterFields = (JSONObject) master.get("fields");
        //System.out.println(masterFields.toJSONString());
        JSONArray rawPNameArray = (JSONArray) masterFields.get("pname.keyword");
        String masterPname = (String) rawPNameArray.get(0);
        hs.add(masterPname);
        ArrayList<String> masterTokens = chopStrings(masterPname);
        for (int k=0;k<dupes.size();k++) {
            JSONObject duplicate = dupes.get(k);
            String duplicateId = (String) duplicate.get(("_id"));
            String duplicateIndex = (String) duplicate.get(("_index"));
            msb.append(mkDupeString(duplicateIndex, duplicateId));
            JSONObject duplicateFields = (JSONObject) duplicate.get("fields");
            rawPNameArray = (JSONArray) duplicateFields.get("pname.keyword");
            String duplicateRawPname = (String) rawPNameArray.get(0);
            hsAdd(hs,duplicateRawPname);
            //System.out.println(duplicateRawPname);
            ArrayList<String> duplicateTokens = chopStrings(duplicateRawPname);
            HashSet<String> duplicateMatches = new HashSet<>();
            for(int i = 0;i<duplicateTokens.size();i++)
                duplicateMatches.add(duplicateTokens.get(i));
            for(int i = masterTokens.size() - 1; i>= 0; i--)
                if(!duplicateMatches.contains(masterTokens.get(i)))
                    masterTokens.remove(i);
        }
        if(masterTokens.size()==0){
            //NOTHING IN COMMON
            return;
        }
        StringBuilder sb = new StringBuilder();
        
        for(int i = 0;i<masterTokens.size();i++){
            if(i>0)
                sb.append(" ");
            sb.append(masterTokens.get(i).substring(0, 1).toUpperCase() + masterTokens.get(i).substring(1));
        }
        String str = mkMasterString(masterIndex,masterId, hs, sb.toString());
            msb.append(str);
        postFromElastic.postDuplicates(ip + "_bulk", msb.toString());
        //System.out.println(msb.toString());
        
        
        
    }
    private static class postFromElastic{
        public static void postDuplicates(String surl, String req) {
            BufferedReader in       = null;
            try {
                byte[] postData       = req.getBytes( StandardCharsets.UTF_8 );
                int    postDataLength = postData.length;
                URL    url = null;
                try {
                    url = new URL( surl );
                } catch (MalformedURLException ex) {
                    Logger.getLogger(DedupeMain.class.getName()).log(Level.SEVERE, null, ex);
                }
                HttpURLConnection conn = null;
                try {
                    conn = (HttpURLConnection) url.openConnection();
                } catch (IOException ex) {
                    Logger.getLogger(DedupeMain.class.getName()).log(Level.SEVERE, null, ex);
                }
                conn.setDoOutput( true );
                conn.setInstanceFollowRedirects( false );
                try {
                    conn.setRequestMethod( "POST" );
                } catch (ProtocolException ex) {
                    Logger.getLogger(DedupeMain.class.getName()).log(Level.SEVERE, null, ex);
                }
                conn.setRequestProperty( "Content-Type", "application/json");
                conn.setRequestProperty( "charset", "utf-8");
                conn.setRequestProperty( "Content-Length", Integer.toString( postDataLength ));
                conn.setUseCaches( false );
                try( DataOutputStream wr = new DataOutputStream( conn.getOutputStream())) {
                    wr.write(postData );
                    wr.flush();
                    wr.close();
                } catch (IOException ex) {
                    Logger.getLogger(DedupeMain.class.getName()).log(Level.SEVERE, null, ex);
                }
                //System.out.println(conn.getResponseMessage() + " " + postDataLength);
                in = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                conn.disconnect();
                //print result
                String output = response.toString();
                //System.out.println(output + " " + postDataLength);
            } catch (IOException ex) {
                Logger.getLogger(DedupeMain.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                try {
                    in.close();
                    
                } catch (IOException ex) {
                    Logger.getLogger(DedupeMain.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            
        }
    }
    public static String mkDupeString(String indexname, String id){
        String template = "{\"update\":{\"_id\":\"$ID\", \"_type\":\"product\", \"_index\":\"$INDEX\"}}\n" +
"{\"doc\":{\"isduplicate\":true}}\n";
        return template.replace("$ID", id).replace("$INDEX", indexname);
    }
    public static String mkMasterString(String indexname, String id, HashSet<String> hs, String pname){
        Iterator<String> iter = hs.iterator();
        boolean isFirst = true;
        StringBuilder msb = new StringBuilder();
        msb.append("[");
        while(iter.hasNext()){
            if(isFirst)
                isFirst = false;
            else{
                msb.append(",");
            }
            String str = iter.next();
            String nxt = "\"" + str.replace("\"","") + "\"";
            msb.append(nxt);
        }
        msb.append("]");
        String template = "{\"update\":{\"_id\":\"$ID\", \"_type\":\"product\", \"_index\":\"$INDEX\"}}\n" +
"{\"doc\":{\"searchPname\":$PNAME}, \"pname\":\"" + pname.replace("\"","\\\"") +"\"}\n";
        return template.replace("$ID", id).replace("$INDEX", indexname).replace("$PNAME", msb.toString());
    }
}
