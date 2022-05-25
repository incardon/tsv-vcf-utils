package com.ukb.IGSB.TsvVcfUtils.score_server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.ukb.IGSB.TsvVcfUtils.TsvVcfUtilsException;
import com.ukb.IGSB.TsvVcfUtils.init_db.TsvVcfFileImporter;
import htsjdk.samtools.util.CloseableIterator;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ScoreServer {

  HttpServer server;

  VCFFileReader fileReader;

  /**
   * Start a vcf database HTTP server
   *
   * @param VcfDatabase Paths to vcf database
   */
  public ScoreServer(String VcfDatabase) throws Exception, IOException {

    server = HttpServer.create(new InetSocketAddress(8000), 0);

    fileReader = new VCFFileReader(new File(VcfDatabase));

    server.createContext("/query", new ProcessHandler(fileReader));
    server.setExecutor(null); // creates a default executor
  }

  /** Execute TSV file import. */
  public void run() throws TsvVcfUtilsException {

    server.start();
  }

  static class ProcessHandler implements HttpHandler {

    VCFFileReader fileReader;

    ProcessHandler(VCFFileReader fileReader) {
      this.fileReader = fileReader;
    }

    private void process(String request, HttpExchange t) throws TsvVcfUtilsException, IOException {

      // Convert request as json

      JSONObject response = new JSONObject();
      JSONArray arr_response = new JSONArray();
      response.put("responses", arr_response);

      try {
        JSONObject jsonObject = new JSONObject(request);
        JSONArray arr = jsonObject.getJSONArray("requests");

        for (int i = 0; i < arr.length(); i++) {
          JSONObject r = arr.getJSONObject(i);

          String chr = r.get("chromosome").toString();
          int chr_int = TsvVcfFileImporter.getChrInteger(chr);
          if (chr_int < 0) {
            throw new TsvVcfUtilsException("Invalid request");
          }

          String ref = null;
          if (r.has("ref")) {
            ref = r.get("ref").toString();
          }

          String alt = null;
          if (r.has("alt")) {
            alt = r.get("alt").toString();
          }

          int start = Integer.parseInt(r.get("start").toString());

          CloseableIterator<VariantContext> it = fileReader.query(chr, start, start);

          JSONObject jo = new JSONObject();
          arr_response.put(jo);
          JSONArray querry_answer = new JSONArray();
          jo.put("annotations", querry_answer);

          VariantContext vc;
          while ((vc = it.next()) != null) {

            if (ref != null) {
              if (!vc.getReference().getBaseString().equals(ref)) {
                continue;
              }
            }

            for (Allele al : vc.getAlleles()) {

              if (alt != null) {
                if (!al.getBaseString().equals(alt)) {
                  continue;
                }
              }

              JSONObject ob_c = new JSONObject();
              ob_c.put("chromosome", vc.getContig());
              ob_c.put("start", vc.getStart());
              ob_c.put("stop", vc.getEnd());
              ob_c.put("ref", vc.getReference().getBaseString());
              ob_c.put("alt", al.getBaseString());

              for (Map.Entry<String, Object> e : vc.getAttributes().entrySet()) {

                ob_c.put(e.getKey(), e.getValue().toString());
              }

              querry_answer.put(ob_c);
            }
          }
        }
      } catch (JSONException err) {
        throw new TsvVcfUtilsException("Invalid request" + request);
      }

      String rep = response.toString() + "\n";

      t.sendResponseHeaders(200, rep.length());
      OutputStream os = t.getResponseBody();
      os.write(rep.toString().getBytes());
      os.close();
    }

    @Override
    public void handle(HttpExchange t) throws IOException {
      String response = "";

      InputStream is = t.getRequestBody();
      String request = new String();

      int b = 0;
      while ((b = is.read()) > 0) {

        request += Character.toString((char) b);
      }

      try {
        process(request, t);
      } catch (TsvVcfUtilsException e) {

        String error = new String("Invalid request\n");
        t.sendResponseHeaders(502, error.length());
        OutputStream os = t.getResponseBody();
        os.write(error.toString().getBytes());
        os.close();
        return;
      }
    }
  }
}
