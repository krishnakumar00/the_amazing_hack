/* Generated By:JavaCC: Do not edit this line. UserQuery.java */
package com.amazon.zama_edoc;

import java.util.HashMap;
import java.io.ByteArrayInputStream;

public class UserQuery implements UserQueryConstants {
  public static String queryString = "";
  public static HashMap obj = new HashMap();
  public String userStr = "";
  public static void main(String args[]) {
    try {
      String attr = new UserQuery(System.in).Input();
      System.out.println("QUERY : select * from table where " + attr + queryString + ";");
    } catch (ParseException e) {
      System.out.println("[error] : "+e.getMessage());
      e.printStackTrace();
    }
  }

  public UserQuery (String inptStr) throws Exception
  {
    String attr = new UserQuery(new ByteArrayInputStream(inptStr.getBytes())).Input();
    userStr = attr + queryString;
  }

  public String getQuery ()
  {
    return userStr;
  }

  final public String Input() throws ParseException {
  String attr = "";
    attr = Attribute();
    label_1:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case SPACE:
        ;
        break;
      default:
        jj_la1[0] = jj_gen;
        break label_1;
      }
      Join();
    }
    label_2:
    while (true) {
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 6:
      case 7:
        ;
        break;
      default:
        jj_la1[1] = jj_gen;
        break label_2;
      }
      switch ((jj_ntk==-1)?jj_ntk():jj_ntk) {
      case 6:
        jj_consume_token(6);
        break;
      case 7:
        jj_consume_token(7);
        break;
      default:
        jj_la1[2] = jj_gen;
        jj_consume_token(-1);
        throw new ParseException();
      }
    }
    {if (true) return attr;}
    throw new Error("Missing return statement in function");
  }

  final public void Join() throws ParseException {
    Token join = null;
    String attr = "";
    obj = new HashMap();
    jj_consume_token(SPACE);
    join = jj_consume_token(JOIN);
    jj_consume_token(SPACE);
    attr = Attribute();
    queryString += " " + join.toString() + attr;
  }

  final public String Attribute() throws ParseException {
  Token name = null;
  Token value = null;
  Token operator = null;
    name = jj_consume_token(NAME);
    jj_consume_token(SPACE);
    operator = jj_consume_token(OPERATOR);
    jj_consume_token(SPACE);
    value = jj_consume_token(VALUE);
    String attrName = name.toString();
    String attrValue = value.toString();
    String oprValue = operator.toString();

    attrValue = attrName.equals("item_id") || attrName.equals("list_price") ? attrValue.replace("\u005c"", "") : attrValue;

    //replace all operators

    oprValue = oprValue.equals("equals") ? "=" : oprValue;
    oprValue = oprValue.equals("not equals") ? "!=" : oprValue;
    oprValue = oprValue.equals("higher than") ? ">=" : oprValue;
    oprValue = oprValue.equals("lower than") ? "<=" : oprValue;

        attrValue = attrValue.replace("'", "\u005c\u005c'");
    attrValue = attrValue.replace("\u005c"", "''");

    {if (true) return " "+attrName+ oprValue +attrValue+"";}
    throw new Error("Missing return statement in function");
  }

  /** Generated Token Manager. */
  public UserQueryTokenManager token_source;
  SimpleCharStream jj_input_stream;
  /** Current token. */
  public Token token;
  /** Next token. */
  public Token jj_nt;
  private int jj_ntk;
  private int jj_gen;
  final private int[] jj_la1 = new int[3];
  static private int[] jj_la1_0;
  static {
      jj_la1_init_0();
   }
   private static void jj_la1_init_0() {
      jj_la1_0 = new int[] {0x8,0xc0,0xc0,};
   }

  /** Constructor with InputStream. */
  public UserQuery(java.io.InputStream stream) {
     this(stream, null);
  }
  /** Constructor with InputStream and supplied encoding */
  public UserQuery(java.io.InputStream stream, String encoding) {
    try { jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source = new UserQueryTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 3; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream) {
     ReInit(stream, null);
  }
  /** Reinitialise. */
  public void ReInit(java.io.InputStream stream, String encoding) {
    try { jj_input_stream.ReInit(stream, encoding, 1, 1); } catch(java.io.UnsupportedEncodingException e) { throw new RuntimeException(e); }
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 3; i++) jj_la1[i] = -1;
  }

  /** Constructor. */
  public UserQuery(java.io.Reader stream) {
    jj_input_stream = new SimpleCharStream(stream, 1, 1);
    token_source = new UserQueryTokenManager(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 3; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  public void ReInit(java.io.Reader stream) {
    jj_input_stream.ReInit(stream, 1, 1);
    token_source.ReInit(jj_input_stream);
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 3; i++) jj_la1[i] = -1;
  }

  /** Constructor with generated Token Manager. */
  public UserQuery(UserQueryTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 3; i++) jj_la1[i] = -1;
  }

  /** Reinitialise. */
  public void ReInit(UserQueryTokenManager tm) {
    token_source = tm;
    token = new Token();
    jj_ntk = -1;
    jj_gen = 0;
    for (int i = 0; i < 3; i++) jj_la1[i] = -1;
  }

  private Token jj_consume_token(int kind) throws ParseException {
    Token oldToken;
    if ((oldToken = token).next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    if (token.kind == kind) {
      jj_gen++;
      return token;
    }
    token = oldToken;
    jj_kind = kind;
    throw generateParseException();
  }


/** Get the next Token. */
  final public Token getNextToken() {
    if (token.next != null) token = token.next;
    else token = token.next = token_source.getNextToken();
    jj_ntk = -1;
    jj_gen++;
    return token;
  }

/** Get the specific Token. */
  final public Token getToken(int index) {
    Token t = token;
    for (int i = 0; i < index; i++) {
      if (t.next != null) t = t.next;
      else t = t.next = token_source.getNextToken();
    }
    return t;
  }

  private int jj_ntk() {
    if ((jj_nt=token.next) == null)
      return (jj_ntk = (token.next=token_source.getNextToken()).kind);
    else
      return (jj_ntk = jj_nt.kind);
  }

  private java.util.List<int[]> jj_expentries = new java.util.ArrayList<int[]>();
  private int[] jj_expentry;
  private int jj_kind = -1;

  /** Generate ParseException. */
  public ParseException generateParseException() {
    jj_expentries.clear();
    boolean[] la1tokens = new boolean[8];
    if (jj_kind >= 0) {
      la1tokens[jj_kind] = true;
      jj_kind = -1;
    }
    for (int i = 0; i < 3; i++) {
      if (jj_la1[i] == jj_gen) {
        for (int j = 0; j < 32; j++) {
          if ((jj_la1_0[i] & (1<<j)) != 0) {
            la1tokens[j] = true;
          }
        }
      }
    }
    for (int i = 0; i < 8; i++) {
      if (la1tokens[i]) {
        jj_expentry = new int[1];
        jj_expentry[0] = i;
        jj_expentries.add(jj_expentry);
      }
    }
    int[][] exptokseq = new int[jj_expentries.size()][];
    for (int i = 0; i < jj_expentries.size(); i++) {
      exptokseq[i] = jj_expentries.get(i);
    }
    return new ParseException(token, exptokseq, tokenImage);
  }

  /** Enable tracing. */
  final public void enable_tracing() {
  }

  /** Disable tracing. */
  final public void disable_tracing() {
  }

}
