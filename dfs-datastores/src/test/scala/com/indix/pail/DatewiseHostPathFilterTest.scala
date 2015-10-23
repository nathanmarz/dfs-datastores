package com.indix.pail

import org.scalatest.{BeforeAndAfter, FunSpec}
import org.scalatest.matchers.{BeMatcher, ShouldMatchers}
import org.scala_tools.time.StaticDateTime._
import com.indix.models.pail.DateHelper

class DatewiseHostPathFilterTest extends FunSpec with ShouldMatchers with BeforeAndAfter {

  describe("Datewise host path filter") {
    it("should return correct pattern for archive dir"){
      val numDays = 10
      val today = now
      val weekRanges = (0 to numDays).map(a => DateHelper.weekInterval(today.minusDays(a))) toSet
      val input = List(("www.amazon.com",numDays))
      val filter = new DatewiseHostPathFilter(input, "/products/master")
      val pathPatterns = filter.getPathPatterns
      pathPatterns.size should be(weekRanges.size)
    }

    it("should return correct pattern for new dir"){
      val numDays = 10
      val today = now
      val dayRanges = ((1 to numDays).map(a => today.minusDays(a).toString("YYYYMMdd")) ++ (0 to now.getHourOfDay-1)) toSet
      val input = List(("www.amazon.com",numDays))
      val filter = new DatewiseHostPathFilter(input, "/products/new")
      val pathPatterns = filter.getPathPatterns
      pathPatterns.size should be(dayRanges.size)
    }
  }

}
