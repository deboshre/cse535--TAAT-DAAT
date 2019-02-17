/*Reference from : http://lucene.apache.org/core/7_3_0/core/org/apache/lucene/index/IndexReader.html */

import java.util.*;
import java.io.*;
import java.nio.*;
import java.nio.file.*;
import static java.nio.charset.StandardCharsets.*;
import java.nio.charset.Charset;

import org.apache.lucene.index.*;

import org.apache.lucene.document.Document;

import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;

public class Indexer {
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final Charset ISO = Charset.forName("ISO-8859-1");
    static LinkedList<Integer> mList = null;
    static HashMap<String, LinkedList<Integer>> InvertedIndex = new HashMap<String, LinkedList<Integer>>();
    public static void main(String[] args) {
        if (args.length != 3) {
            System.exit(1);
        }
        String IndexPath = args[0];
        String OutputPath = args[1];
        String InputPath = args[2];
        System.out.println(IndexPath);
        System.out.println(OutputPath);
        System.out.println(InputPath);
        try {
            InvertedIndex(IndexPath);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        ExecuteQuery(InputPath, OutputPath);
    }

    public static HashMap InvertedIndex(String IndexPath) throws IOException{

        Path index_path = Paths.get(IndexPath);
        Directory index_directory = FSDirectory.open(index_path);
        IndexReader index_reader = DirectoryReader.open(index_directory);

        Fields index_fields = MultiFields.getFields(index_reader);
        Iterator fieldIterator = index_fields.iterator();
        ArrayList<String> termslist = new ArrayList<>();
        while(fieldIterator.hasNext()) {
            String field = (String) fieldIterator.next();
            if(field.equals("id"))
                continue;
            System.out.println(field);
            Terms terms = MultiFields.getTerms(index_reader, field);
            TermsEnum terms_num = terms.iterator();
            while(terms_num.next() != null) {
                CharsRefBuilder spare = new CharsRefBuilder();
                LinkedList<Integer> postings = new LinkedList<>();
                BytesRef term_to_string = terms_num.term();
                PostingsEnum docs_num = MultiFields.getTermDocsEnum(index_reader, field, term_to_string);
                int docid = docs_num.nextDoc();
                while(docid != docs_num.NO_MORE_DOCS) {
                    postings.add(docid);
                    docid = docs_num.nextDoc();
                }
                spare.copyUTF8Bytes(term_to_string);
                termslist.add(term_to_string.utf8ToString());
                InvertedIndex.put(term_to_string.utf8ToString(), postings);
            }

        }
        index_reader.close();
        return InvertedIndex;
    }
    public static void ExecuteQuery(String InputPath, String OutputPath) {
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader( new FileInputStream(InputPath), "UTF-8"));
            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(OutputPath), "UTF-8"));
            LinkedList<Integer> TaaT1 = null,TaaT2 = null, temp = null;
            LinkedList<Integer> TaatAND = null, TaatOR = null;
            LinkedList<Integer> DaatAND = null, DaatOR = null;
            int count_daat_and,count_daat_or, count_taat_or, count_taat_and;

            String input = "";
            while((input = br.readLine()) != null) {
                String[] queryTerm = input.split(" "); //splitting the query term
                String[] queryTermConverted = new String[queryTerm.length];
                count_daat_and = count_daat_or = count_taat_or = count_taat_and = 0;
                for(int i=0;i<queryTerm.length;i++) { //getting each query term
                    queryTermConverted[i] = new String(queryTerm[i].getBytes(ISO), UTF_8);
                    writer.write("GetPostings");
                    writer.write('\n');
                    writer.write(queryTerm[i]);
                    writer.write('\n');
                    temp = InvertedIndex.get(queryTermConverted[i]); //returning the postings list for the query term
                    writer.write("Postings list:"); //
                    for(int j=0;j<temp.size();j++)
                        writer.write(" "+temp.get(j));
                    writer.write('\n');
                }
                TaatAND = InvertedIndex.get(queryTermConverted[0]);
                TaatOR = InvertedIndex.get(queryTermConverted[0]);
                for(int i=1;i<queryTerm.length;i++) {
                    TaaT2 = InvertedIndex.get(queryTermConverted[i]);

                    TaaT1 = TaatAND;
                    TaatAND=null;
                    TaatAND = new LinkedList<Integer>();
                    count_taat_and += Compare_TAAT_AND(TaaT1, TaaT2, TaatAND);

                    TaaT1 = TaatOR;
                    TaatOR=null;
                    TaatOR = new LinkedList<Integer>();
                    count_taat_or += Compare_TAAT_OR(TaaT1, TaaT2, TaatOR);
                }

                //printing the result for TAAT AND
                writer.write("TaatAnd");
                writer.write('\n');
                for(int i=0;i<queryTerm.length;i++) {
                    writer.write(queryTerm[i]);
                    if(i != queryTerm.length-1 )
                        writer.write(" ");
                }
                writer.write('\n');
                writer.write("Results:");
                if(TaatAND.size() == 0){
                    writer.write(" empty");
                    writer.write('\n');
                }
                else {
                    for(int j=0;j<TaatAND.size();j++)
                        writer.write(" "+TaatAND.get(j));
                    writer.write('\n');
                }
                writer.write("Number of documents in results: "+TaatAND.size());
                writer.write('\n');
                writer.write("Number of comparisons: "+count_taat_and);
                writer.write('\n');

                //printing the result for taat or
                writer.write("TaatOr");
                writer.write('\n');
                for(int i=0;i<queryTerm.length;i++) {
                    writer.write(queryTerm[i]);
                    if(i != queryTerm.length-1 )
                        writer.write(" ");
                }
                writer.write('\n');
                writer.write("Results:");
                if(TaatOR.size() == 0) {
                    writer.write(" empty list");
                    writer.write('\n');
                }
                else {
                    for(int j=0;j<TaatOR.size();j++)
                        writer.write(" "+TaatOR.get(j));
                    writer.write('\n');
                }
                writer.write("Number of documents in results: "+TaatOR.size());
                writer.write('\n');
                writer.write("Number of comparisons: "+count_taat_or);
                writer.write('\n');

                // DAAT AND
                DaatAND = null;
                DaatAND = new LinkedList<Integer>();
                count_daat_and = Compare_DAAT_AND(queryTermConverted, DaatAND);
                writer.write("DaatAnd");
                writer.write('\n');
                for(int i=0;i<queryTerm.length;i++) {
                    writer.write(queryTerm[i]);
                    if(i != queryTerm.length-1 )
                        writer.write(" ");
                }
                writer.write('\n');
                writer.write("Results:");
                if(DaatAND.size() == 0) {
                    writer.write(" empty");
                    writer.write('\n');
                }
                else {
                    for(int j=0;j<DaatAND.size();j++)
                        writer.write(" "+DaatAND.get(j));
                    writer.write('\n');
                }
                writer.write("Number of documents in results: "+DaatAND.size());
                writer.write('\n');
                writer.write("Number of comparisons: "+count_daat_and);
                writer.write('\n');

                //DAAT OR
                DaatOR=null;
                DaatOR = new LinkedList<Integer>();
                count_daat_or = Compare_DAAT_OR(queryTermConverted, DaatOR);
                writer.write("DaatOr");writer.write('\n');
                for(int i=0;i<queryTerm.length;i++) {
                    writer.write(queryTerm[i]);
                    if(i != queryTerm.length-1 )
                        writer.write(" ");
                }
                writer.write('\n');
                writer.write("Results:");
                if(DaatOR.size() == 0) {
                    writer.write(" empty");writer.write('\n');
                }
                else {
                    for(int j=0;j<DaatOR.size();j++)
                        writer.write(" "+DaatOR.get(j));
                    writer.write('\n');
                }
                writer.write("Number of documents in results: "+ DaatOR.size());writer.write('\n');
                writer.write("Number of comparisons: "+ count_daat_or);writer.write('\n');
            }
            writer.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }

    static int compare(int a, int b)
    {
        if (a==b)
            return 0;
        else if (a<b)
            return 1;
        else
            return 2;
    }
    //check if all the postings are completed
    private static boolean isCompleteEmpty(ArrayList<LinkedList<Integer>> any_Array) {
        for(int i=0;i<any_Array.size();i++) {
            if(any_Array.get(i).size() == 0) {
                any_Array.remove(i);
                i--;
                }
            }
        if(any_Array.size() == 1 || any_Array.size() == 0)
            return true;
        else return false;
        }

    public static int nextSkip(int size, int index) {
        int root = (int) Math.sqrt(size);
        return (index + root);
    }

    public static boolean hasSkip(int size, int index) {
        int root = (int) Math.sqrt(size);
        return root != 1 && index % root == 0 && index + root < size;
    }

    public static int skip_pointer_intersection(LinkedList<Integer> m, LinkedList<Integer> n, LinkedList<Integer> result) {
        LinkedList<Integer> a = (LinkedList<Integer>) m.clone();
        LinkedList<Integer> b = (LinkedList<Integer>) n.clone();
        int i = 0, j = 0, count = 0;
        while ( i < a.size() && j < b.size()) {
            count++;
            if((int)a.get(i) == (int)b.get(j)) {
                result.add(a.get(i));
                i++; j++;
            } else if ((int)a.get(i) < (int)b.get(j)) {
                if (hasSkip(a.size(), i) && (int) a.get(nextSkip(a.size(), i)) < (int)b.get(j)) {
                    while (hasSkip(a.size(), i) && (int) a.get(nextSkip(a.size(), i)) < (int)b.get(j)) {
                        i = nextSkip(a.size(), i);
                    }
                } else {
                    i++;
                }
            } else if (hasSkip(b.size(), j) && (int)b.get(nextSkip(b.size(), j)) < (int)a.get(i)) {
                while (hasSkip(b.size(), j) && (int)b.get(nextSkip(b.size(), j)) < (int)a.get(i)) {
                    j = nextSkip(a.size(), j);
                }
            } else {
                j++;
            }
        }
        return count;
    }

    private static int Compare_DAAT_AND(String[] queryTerm, LinkedList<Integer> DaatAND)
    {
        int i=0,j=0,count=0;
        LinkedList<Integer> PostingList = null;
        ArrayList<LinkedList<Integer>> new_Array = new ArrayList<LinkedList<Integer>>();
        for(String s:queryTerm) {
            PostingList = new LinkedList<Integer>();
            PostingList = (LinkedList<Integer>) InvertedIndex.get(s).clone();
            new_Array.add(PostingList);
        }
        if(queryTerm.length == 1) {
            LinkedList<Integer> temp = (LinkedList<Integer>) InvertedIndex.get(queryTerm[0]).clone();
            for(i=0;i<temp.size();i++)
                DaatAND.add(temp.get(i));
            return 0;
        } else {
            if (queryTerm.length == 2) {
                LinkedList<Integer> result = new LinkedList<Integer>();
                count = skip_pointer_intersection(new_Array.get(0), new_Array.get(1), result);
                for(int k = 0; k < result.size(); k++) {
                    DaatAND.add(result.get(k));
                }
            } else {
                int k = 1;
                LinkedList<Integer> result = new LinkedList<Integer>();
                while (k < new_Array.size()) {
                    if(k == 1) {
                        count = skip_pointer_intersection(new_Array.get(k-1), new_Array.get(k), result);
                        k++;
                    } else {
                        count += skip_pointer_intersection(result, new_Array.get(k), result);
                        k++;
                    }
                }
                for(int m = 0; m < result.size(); m++) {
                    DaatAND.add(result.get(m));
                }
            }
        }
        return count;
    }

    private static int Compare_DAAT_OR(String[] queryTerm, LinkedList<Integer> DaatOR)
    {
        int count= 0;
        int i=0,j=0,result;
        LinkedList<Integer> PostingList = null;
        ArrayList<LinkedList<Integer>> new_Array = new ArrayList<LinkedList<Integer>>();
        for(String s:queryTerm) {
            PostingList = new LinkedList<Integer>();
            PostingList = (LinkedList<Integer>) InvertedIndex.get(s).clone();
            new_Array.add(PostingList);
        }
        if(queryTerm.length == 1) {
            LinkedList<Integer> temp = (LinkedList<Integer>) InvertedIndex.get(queryTerm[0]).clone();
            for(i=0;i<temp.size();i++)
                DaatOR.add(temp.get(i));
            return 0;
        }
        while(!isCompleteEmpty(new_Array)) {
            for(i=0,j=1; i<new_Array.size()-1 && j<new_Array.size();) {
                count++;
                result = compare(new_Array.get(i).get(0), new_Array.get(j).get(0));
                if(result == 0) {
                    new_Array.get(i).remove(0);
                    i = j;j++;
                } else {
                    if(result == 1) {
                        j=j+1;
                    }
                    else {
                        i = j;j++;
                    }
                }
            }
            DaatOR.add(new_Array.get(i).get(0));
            new_Array.get(i).remove(0);
        }
        while(new_Array!= null && new_Array.size() != 0 && new_Array.get(0) != null && new_Array.get(0).size() != 0) {
            DaatOR.add(new_Array.get(0).get(0));
            new_Array.get(0).remove(0);
        }
    return count;
    }

    private static int Compare_TAAT_AND(LinkedList<Integer> TaaT1, LinkedList<Integer> TaaT2,
            LinkedList<Integer> TaatAND)
    {
        int count= 0;
        LinkedList<Integer> result = new LinkedList<Integer>();
        count = skip_pointer_intersection(TaaT1, TaaT2, result);
        for(int i = 0; i < result.size(); i++) {
            TaatAND.add(result.get(i));
        }
        return count;
    }
    private static int Compare_TAAT_OR(LinkedList<Integer> TaaT1, LinkedList<Integer> TaaT2,
            LinkedList<Integer> TaatOR)
    {
        int count= 0;
        int i=0,j=0,counter=0,result;
        while(i<TaaT1.size() && j<TaaT2.size()) {
            counter++;
            result = compare(TaaT1.get(i),TaaT2.get(j));
            if(result == 0) {
                TaatOR.add(TaaT1.get(i));
                i++;j++;
            } else if(result == 1) {
                TaatOR.add(TaaT1.get(i));
                i++;
            }
            else {
                TaatOR.add(TaaT2.get(j));
                j++;
            }
        }
        while(i<TaaT1.size()) {
            TaatOR.add(TaaT1.get(i));
            i++;
        }
        while(j<TaaT2.size()) {
            TaatOR.add(TaaT2.get(j));
            j++;
        }
    return count;
    }
}
