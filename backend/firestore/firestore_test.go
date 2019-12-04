package firestore

import (
	. "gopkg.in/check.v1"

	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}

type FirebaseSuite struct{}

var _ = Suite(&FirebaseSuite{})

func (s *FirebaseSuite) TestSplitAddress(c *C) {
	proj, col := splitEndpoint("project/collection")
	c.Assert(proj, Equals, "project")
	c.Assert(col, Equals, "collection")

	proj, col = splitEndpoint("project/collection/")
	c.Assert(proj, Equals, "project")
	c.Assert(col, Equals, "collection")

	proj, col = splitEndpoint("project/collection1/document/collection2")
	c.Assert(proj, Equals, "project")
	c.Assert(col, Equals, "collection1/document/collection2")

	proj, col = splitEndpoint("project/collection1/document/collection2/")
	c.Assert(proj, Equals, "project")
	c.Assert(col, Equals, "collection1/document/collection2")
}
