
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

struct StrListTest {
    1: required list<string> lst;
}

struct MapTest {
    1: required map<string, i32> m;
}

struct SetTest {
    1: required set<i32> s;
}

struct PersonTest {
    1: required string name;
    2: required i32 age;
    3: required double height;
}

struct CoordinateTest {
    1: required i32 x;
    2: required i32 y;
}

struct CoordinateListsTest {
    1: required list<CoordinateTest> coords;
}