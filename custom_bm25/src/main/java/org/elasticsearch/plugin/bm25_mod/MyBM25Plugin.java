package org.elasticsearch.plugin.bm25_mod;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
//import org.elasticsearch.script.FilterScript;
//import org.elasticsearch.script.FilterScript.LeafFactory;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.ScoreScript.LeafFactory;

/**
 * A script for finding documents that match a term a certain number of times
 */
public class MyBM25Plugin extends Plugin implements ScriptPlugin {

    @Override
    public ScriptEngine getScriptEngine(
        Settings settings,
        Collection<ScriptContext<?>> contexts
    ) {
        return new MyBM25ScriptEngine();
    }

    public static class MyBM25ScriptEngine implements ScriptEngine {

        @Override
        public String getType() {
            return "bm25_scriptengine";
        }

        @Override
        public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
            if (!context.equals(ScoreScript.CONTEXT)) {
                throw new IllegalArgumentException(getType()
                        + " scripts cannot be used for context ["
                        + context.name + "]");
            }

            if ("bm25_custom".equals(scriptSource)) {
                ScoreScript.Factory factory = new CustomBM25Factory();
                return context.factoryClazz.cast(factory);
            }

            throw new IllegalArgumentException("Unknown script name " + scriptSource);
        }

        @Override
        public void close() {
            // optionally close resources
        }

        @Override
        public Set<ScriptContext<?>> getSupportedContexts() {
            return Set.of(ScoreScript.CONTEXT);
        }

        private static class CustomBM25Factory implements ScoreScript.Factory, ScriptFactory {
            @Override
            public boolean isResultDeterministic() {
                // If the script only uses deterministic APIs, this should return true
                return true;
            }

            @Override
            public LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup) {
                return new CustomBM25LeafFactory(params, lookup);
            }
        }

        private static class CustomBM25LeafFactory implements LeafFactory {
            private final Map<String, Object> params;
            private final SearchLookup lookup;
            private final String field;
            private final String term;
            private final String[] query_terms;

            private CustomBM25LeafFactory(Map<String, Object> params, SearchLookup lookup) {
                if (params.containsKey("field") == false) {
                    throw new IllegalArgumentException("Missing parameter [field]");
                }
                if (params.containsKey("term") == false) {
                    throw new IllegalArgumentException("Missing parameter [term]");
                }
                this.params = params;
                this.lookup = lookup;
                field = params.get("field").toString();
                term = params.get("term").toString();
                query_terms = term.split("\\s+");

            }

            @Override
            public boolean needs_score() {
                return true;
            }

            // @Override
            // public ScoreScript newInstance(LeafReaderContext context) throws IOException {
            //     return new ScoreScript(params, lookup, context) {
            //         double k1 = 1.2;
            //         double b = 0.75;
            //         Terms terms = context.reader().terms(field);
            //         long N = context.reader().maxDoc();
            //         double avgdl = context.reader().getSumTotalTermFreq(field) / (double) N;

            //         @Override
            //         public double execute(ExplanationHolder explanation) {
            //             try {
            //                 // Retrieve the term frequency in the document
            //                 PostingsEnum postings = MultiTerms.getTermPostingsEnum(context.reader(), field, new BytesRef(term), PostingsEnum.FREQS);
            //                 postings.advance(docid);
            //                 double tf = postings.freq();

            //                 // Retrieve the number of documents containing the term
            //                 double n = terms.docCount();

            //                 // Compute IDF
            //                 double idf = Math.log(1 + (N - n + 0.5) / (n + 0.5));

            //                 // Compute the document length
            //                 NumericDocValues norms = context.reader().getNormValues(field);
            //                 double dl = norms.advanceExact(docid) ? norms.longValue() : 0;

            //                 // Compute the BM25 score
            //                 double score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl)));
            //                 return score;
            //             } catch (IOException e) {
            //                 throw new UncheckedIOException(e);
            //             }
            //         }
            //     };
            // }


            // working for first doc

            // @Override
            // public ScoreScript newInstance(LeafReaderContext context) throws IOException {
            //     return new ScoreScript(params, lookup, context) {
            //         double k1 = 1.2;
            //         double b = 0.75;
            //         Terms terms = context.reader().terms(field);
            //         long N = context.reader().maxDoc();
            //         double avgdl = context.reader().getSumTotalTermFreq(field) / (double) N;

            //         @Override
            //         public double execute(ExplanationHolder explanation) {
            //             try {
            //                 // Retrieve the term frequency in the document
            //                 PostingsEnum postings = MultiTerms.getTermPostingsEnum(context.reader(), field, new BytesRef(term), PostingsEnum.FREQS);
            //                 postings.advance(postings.docID());
            //                 double tf = postings.freq();

            //                 // Retrieve the number of documents containing the term
            //                 TermsEnum termsEnum = terms.iterator();
            //                 if (termsEnum.seekExact(new BytesRef(term))) {
            //                     long n = termsEnum.docFreq();

            //                     // Compute IDF
            //                     double idf = Math.log(1 + (N - n + 0.5) / (n + 0.5));

            //                     // Compute the document length
            //                     NumericDocValues norms = context.reader().getNormValues(field);
            //                     double dl = norms.advanceExact(postings.docID()) ? norms.longValue() : 0;

            //                     // Compute the BM25 score
            //                     //double score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl)));
            //                     //double score = avgdl;
            //                     double score = idf;
            //                     return score;
            //                 } else {
            //                     return 0.0;
            //                 }
            //             } catch (IOException e) {
            //                 throw new UncheckedIOException(e);
            //             }
            //         }
            //     };
            // }


            // @Override
            // public ScoreScript newInstance(LeafReaderContext context) throws IOException {
            //     PostingsEnum postings = MultiTerms.getTermPostingsEnum(context.reader(), field, new BytesRef(term), PostingsEnum.FREQS);

            //     if (postings == null) {
            //         /*
            //          * the field and/or term don't exist in this segment,
            //          * so always return 0
            //          */
            //         return new ScoreScript(params, lookup, context) {
            //             @Override
            //             public double execute(ExplanationHolder explanation) {
            //                 return 0.0;
            //             }
            //         };
            //     }

            //     return new ScoreScript(params, lookup, context) {
            //         double k1 = 1.2;
            //         double b = 0.75;
            //         Terms terms = context.reader().terms(field);
            //         long N = context.reader().maxDoc();
            //         double avgdl = context.reader().getSumTotalTermFreq(field) / (double) N;

            //         int currentDocid = -1;
            //         @Override
            //         public void setDocument(int docid) {
            //             /*
            //              * advance has undefined behavior calling with
            //              * a docid <= its current docid
            //              */
            //             if (postings.docID() < docid) {
            //                 try {
            //                     postings.advance(docid);
            //                 } catch (IOException e) {
            //                     throw new UncheckedIOException(e);
            //                 }
            //             }
            //             currentDocid = docid;
            //         }

            //         @Override
            //         public double execute(ExplanationHolder explanation) {
            //             // if (postings.docID() != currentDocid) {
            //             //     /*
            //             //      * advance moved past the current doc, so this
            //             //      * doc has no occurrences of the term
            //             //      */
            //             //     return 0.0;
            //             // }
            //             try {
            //                 // Retrieve the term frequency in the document
            //                 double tf = postings.freq();
            //                 postings.advance(currentDocid);
            //                 //postings.advance(postings.docID());
            //                 //double tf = postings.freq();

            //                 // Retrieve the number of documents containing the term
            //                 TermsEnum termsEnum = terms.iterator();
            //                 if (termsEnum.seekExact(new BytesRef(term))) {
            //                     long n = termsEnum.docFreq();

            //                     // Compute IDF
            //                     double idf = Math.log(1 + (N - n + 0.5) / (n + 0.5));

            //                     // Compute the document length
            //                     NumericDocValues norms = context.reader().getNormValues(field);
            //                     double dl = norms.advanceExact(currentDocid) ? norms.longValue() : 0;
            //                     //double dl = norms.advanceExact(postings.docID()) ? norms.longValue() : 0;

            //                     // Compute the BM25 score
            //                     //double score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl)));
            //                     //double score = avgdl;
            //                     //double score = dl;
            //                     double score = tf;
            //                     return score;
            //                 } else {
            //                     return 0.0;
            //                 }
            //             } catch (IOException e) {
            //                 throw new UncheckedIOException(e);
            //             }
            //         }
            //     };
            // }

            @Override
            public ScoreScript newInstance(LeafReaderContext context) throws IOException {
                /*
                * create an array of PostingsEnums,
                * one for each word in the phrase
                */
                String[] query_terms = term.split("\\s+");

                if (query_terms.length == 0) {
                /*
                * the term did not have any words to search for,
                * so always return 0
                */
                    return new ScoreScript(params, lookup, context) {
                        @Override
                        public double execute(ExplanationHolder explanation) {
                            return 0.0;
                        }
                    };
                }

                PostingsEnum[] postingsArr = new PostingsEnum[query_terms.length];

                for (int i = 0; i < query_terms.length; i++) {
                    String currTerm = query_terms[i];

                    /*
                    * use PostingsEnum.POSITIONS to get the positions
                    * of each occurence of the word in the item
                    */
                    PostingsEnum postings = MultiTerms.getTermPostingsEnum(context.reader(), field, new BytesRef(currTerm), PostingsEnum.FREQS);

                    if (postings == null) {
                        /*
                        * the field and/or term do not exist in this segment,
                        * so always return 0
                        */
                        return new ScoreScript(params, lookup, context) {
                            @Override
                            public double execute(ExplanationHolder explanation) {
                                return 0.0;
                            }
                        };
                    }
                    postingsArr[i] = postings;
                }

                return new ScoreScript(params, lookup, context) {
                    double k1 = 1.2;
                    double b = 0.75;
                    Terms terms = context.reader().terms(field);
                    long N = context.reader().maxDoc();
                    double avgdl = context.reader().getSumTotalTermFreq(field) / (double) N;

                    int currentDocid = -1;

                    @Override
                    public void setDocument(int docid) {
                        /*
                        * advance has undefined behavior calling with
                        * a docid <= its current docid
                        */
                        for (PostingsEnum postings : postingsArr) {
                        /*
                        * set each of the PostingsEnum's in the array to
                        * the current docid
                        */
                        if (postings.docID() < docid) {
                            try {
                                postings.advance(docid);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                        }
                        currentDocid = docid;
                    }

                    @Override
                    public double execute(ExplanationHolder explanation) {

                        try {
                            double score = 0.0;
                            for (int i = 0; i < postingsArr.length; i++) {
                                PostingsEnum postings = postingsArr[i];
                                String currTerm = query_terms[i];

                                // if (postings.docID() != currentDocid) {
                                //     /*
                                //     * advance moved past the current doc, so this
                                //     * doc has no occurrences of the term
                                //     */
                                //     return 0.0;
                                // }

                                // Retrieve the term frequency in the document
                                double tf = postings.freq();
                                double tf1 = tf > 0 ? 1/tf : 0;
                                //postings.advance(currentDocid);

                                // Retrieve the number of documents containing the term
                                TermsEnum termsEnum = terms.iterator();
                                if (termsEnum.seekExact(new BytesRef(currTerm))) {
                                    long n = termsEnum.docFreq();

                                    // Compute IDF
                                    double idf = Math.log(1 + (N - n + 0.5) / (n + 0.5));

                                    // Compute the document length
                                    NumericDocValues norms = context.reader().getNormValues(field);
                                    double dl = norms.advanceExact(currentDocid) ? norms.longValue() : 0;
                                    //double dl = norms.advanceExact(postings.docID()) ? norms.longValue() : 0;

                                    // Compute the BM25 score
                                    //double score = idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl)));
                                    //double score = avgdl;
                                    //double score = dl;
                                    //score += idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl)));
                                    //score += idf * (tf1 * (k1 + 1)) / (tf1 + k1) * Math.log(dl);
                                    score += idf * (tf1 * (k1 + 1)) / (tf1 + k1);
                                }
                            }
                            return score;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }

                    }
                };

            }



        }
    }
}