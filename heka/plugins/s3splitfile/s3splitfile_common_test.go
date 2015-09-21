/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
# ***** END LICENSE BLOCK *****/

package s3splitfile

import (
	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	gs "github.com/rafrombrc/gospec/src/gospec"
	"path/filepath"
)

func testFieldVal(c gs.Context, schema Schema, field string, actual string, expected string) {
	sVal, err := schema.GetValue(field, actual)
	c.Expect(err, gs.IsNil)
	c.Expect(sVal, gs.Equals, expected)
}

func S3SplitFileSpec(c gs.Context) {
	c.Specify("Sanitize dimensions", func() {
		c.Expect("hello_there", gs.Equals, SanitizeDimension("hello!there"))

		c.Expect("___________________________", gs.Equals, SanitizeDimension("!@#$%^&*(){}[]|+=-`~'\",<>?\x02"))
	})

	c.Specify("JSON Schema", func() {
		schema, err := LoadSchema(filepath.Join(".", "testsupport", "schema.json"))
		c.Expect(err, gs.IsNil)

		c.Expect(len(schema.Fields), gs.Equals, 5)

		// Bogus field:
		_, err = schema.GetValue("bogus", "some value")
		c.Expect(err, gs.Not(gs.IsNil))

		testFieldVal(c, schema, "any", "foo", "foo")
		testFieldVal(c, schema, "any", "Any value at all is acceptable!", "Any value at all is acceptable!")

		testFieldVal(c, schema, "list", "foo", "foo")
		testFieldVal(c, schema, "list", "bar", "bar")
		testFieldVal(c, schema, "list", "baz", "baz")
		testFieldVal(c, schema, "list", "quux", "OTHER")
		testFieldVal(c, schema, "list", "Some values are not acceptable!", "OTHER")

		testFieldVal(c, schema, "rangeMin", "aaa", "aaa")
		testFieldVal(c, schema, "rangeMin", "foo", "foo")
		testFieldVal(c, schema, "rangeMin", "bar", "bar")
		testFieldVal(c, schema, "rangeMin", "all values larger than 'aaa' are fine!", "all values larger than 'aaa' are fine!")
		testFieldVal(c, schema, "rangeMin", "100", "OTHER")

		testFieldVal(c, schema, "rangeMax", "all", "all")
		testFieldVal(c, schema, "rangeMax", "bar", "bar")
		testFieldVal(c, schema, "rangeMax", "bbb", "bbb")
		testFieldVal(c, schema, "rangeMax", "all values smaller than 'bbb' are fine!", "all values smaller than 'bbb' are fine!")
		testFieldVal(c, schema, "rangeMax", "100", "100")
		testFieldVal(c, schema, "rangeMax", "ccc", "OTHER")

		testFieldVal(c, schema, "range", "aaa", "aaa")
		testFieldVal(c, schema, "range", "all", "all")
		testFieldVal(c, schema, "range", "bar", "bar")
		testFieldVal(c, schema, "range", "bbb", "bbb")
		testFieldVal(c, schema, "range", "all values between 'aaa' and 'bbb' are fine!", "all values between 'aaa' and 'bbb' are fine!")
		testFieldVal(c, schema, "range", "100", "OTHER")
		testFieldVal(c, schema, "range", "aa0", "OTHER")
		testFieldVal(c, schema, "range", "bbc", "OTHER")
		testFieldVal(c, schema, "range", "ccc", "OTHER")
	})

	c.Specify("Non-string fields", func() {
		schema, _ := LoadSchema(filepath.Join(".", "testsupport", "schema.json"))
		pack := NewPipelinePack(nil)

		// No fields
		dims := schema.GetDimensions(pack)
		c.Expect(dims[0], gs.Equals, "UNKNOWN")

		// Integer field
		f, _ := message.NewField("any", 1, "")
		pack.Message.AddField(f)
		dims = schema.GetDimensions(pack)
		c.Expect(dims[0], gs.Equals, "1")
		pack.Message.DeleteField(f)

		// Boolean field
		f, _ = message.NewField("any", true, "")
		pack.Message.AddField(f)
		dims = schema.GetDimensions(pack)
		c.Expect(dims[0], gs.Equals, "true")
		pack.Message.DeleteField(f)

		// Double field
		f, _ = message.NewField("any", 1.23, "")
		pack.Message.AddField(f)
		dims = schema.GetDimensions(pack)
		c.Expect(dims[0], gs.Equals, "1.23")
		pack.Message.DeleteField(f)

	})
}
