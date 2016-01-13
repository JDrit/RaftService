
namespace java edu.rit.csh.scaladb.serialization
#@namespace scala edu.rit.csh.scaladb.serialization

struct StringTest {
    1: required string str;
}

struct IntTest {
    1: required i32 int;
}

struct ListTest {
    1: required list<i32> lst;
}

struct MapTest {
    1: required map<string, i32> m;
}