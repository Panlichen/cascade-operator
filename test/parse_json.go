package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	// derechov1alpha1 "github.com/Panlichen/cascade_operator/api/v1alpha1"
)

type SimpleType struct {
	TypeAlias string `json:"type_alias"`
}

type SimpleSpec struct {
	TypesSpec []SimpleType `json:"typesSpec"`
}

type response2 struct {
	Page   int      `json:"page"`
	Fruits []string `json:"fruits"`
}

type ResponseArray struct {
	Reses []response2 `json:"reses"`
}

func main() {
	file, _ := os.Open("simple_test.json")
	defer file.Close()

	wr := bytes.Buffer{}
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		wr.WriteString(sc.Text())
	}

	jsonStr := wr.String()
	jsonStr = "{\"typesSpec\": " + jsonStr + "}"
	jsonStr = strings.Replace(jsonStr, " ", "", -1)
	// jsonStr = strings.Replace(jsonStr, "\"", "", -1)
	fmt.Println(jsonStr)

	simpleSpec := SimpleSpec{}
	// simpleSpec.TypesSpec = make([]SimpleType, 0)
	json.Unmarshal([]byte(jsonStr), &simpleSpec)
	fmt.Println(fmt.Sprintf("Unmarshal done, parse %v types", len(simpleSpec.TypesSpec)))
	fmt.Printf("%+v\n", simpleSpec)

	fmt.Println("=========")

	fileContent, _ := ioutil.ReadFile("single_test.json")
	singleStr := string(fileContent)
	fmt.Println(singleStr)
	singleType := SimpleType{}
	json.Unmarshal([]byte(singleStr), &singleType)
	fmt.Printf("%+v\n", singleType)

	fmt.Println("=========")

	str := `{"page": 1, "fruits": ["apple", "peach"]}`
	res := response2{}
	json.Unmarshal([]byte(str), &res)
	fmt.Printf("%+v\n", res)
	fmt.Println(res.Fruits[0])

	fmt.Println("=========")

	// str2 := `[{"page": 1, "fruits": ["apple", "peach"]},{"page": 2, "fruits": ["pear", "orange"]}]`
	// reses := ResponseArray{}
	// json.Unmarshal([]byte(str2), &reses)

}

// func main() {

// 	fContent, _ := ioutil.ReadFile("test.json")
// 	jsonStr := string(fContent)
// 	jsonStr = "\"typesSpec\": " + jsonStr
// 	fmt.Println(jsonStr)

// 	cascadeNodeManagerSpec := new(derechov1alpha1.CascadeNodeManagerSpec)
// 	json.Unmarshal([]byte(jsonStr), cascadeNodeManagerSpec)
// 	fmt.Println(fmt.Sprintf("Unmarshal done, parse %v types", len(cascadeNodeManagerSpec.TypesSpec)))
// }
