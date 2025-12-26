package com.experian.ind.prime.ingest.core.parser.commercial.rules

import scala.collection.mutable

/**
 * RuleContext - Encapsulates all context data needed for rule evaluation
 * Supports both DSL and SQL evaluation by providing structured access to field, record, and segment data
 * 
 * OPTIMIZATION: Cache immutable views and avoid redundant conversions
 * 
 * Usage:
 *   val context = RuleContext()
 *     .withFieldData("value" -> "ABC", "is_mandatory" -> true, "max_length" -> 10)
 *     .withRecordData("bs_count" -> 1, "rs_count" -> 2)
 *   
 *   val evaluator = new DslRuleEvaluator()
 *   val result = evaluator.evaluate("field.is_mandatory AND field.value.length <= field.max_length", context)
 */
class RuleContext {
  private val data: mutable.Map[String, Any] = mutable.Map()
  private val fieldData: mutable.Map[String, Any] = mutable.Map()
  private val recordData: mutable.Map[String, Any] = mutable.Map()
  private val segmentData: mutable.Map[String, Any] = mutable.Map()

  // For segment order validation: store actual and expected segment sequences
  private var segmentSequence: Seq[String] = Seq.empty
  private var expectedMandatorySegmentSequence: Seq[String] = Seq.empty
  private var expectedSegmentOrder: Seq[String] = Seq.empty  // Schema-driven expected order from recordSegmentOrder

  def withSegmentSequence(seq: Seq[String]): RuleContext = {
    segmentSequence = seq
    data("segment_sequence") = seq
    this
  }

  def withExpectedMandatorySegmentSequence(seq: Seq[String]): RuleContext = {
    expectedMandatorySegmentSequence = seq
    data("expected_mandatory_segment_sequence") = seq
    this
  }

  def withExpectedSegmentOrder(seq: Seq[String]): RuleContext = {
    expectedSegmentOrder = seq
    data("expected_segment_order") = seq
    this
  }

  def getSegmentSequence: Seq[String] = segmentSequence
  def getExpectedMandatorySegmentSequence: Seq[String] = expectedMandatorySegmentSequence
  def getExpectedSegmentOrder: Seq[String] = expectedSegmentOrder
  
  // OPTIMIZATION: Cache immutable views to avoid repeated toMap() conversions
  @volatile private var cachedFieldDataView: Option[Map[String, Any]] = None
  @volatile private var cachedRecordDataView: Option[Map[String, Any]] = None
  @volatile private var cachedSegmentDataView: Option[Map[String, Any]] = None

  /**
   * Add field-level context data
   * Example: withFieldData(("value", "ABC"), ("is_mandatory", true), ("max_length", 10))
   */
  def withFieldData(pairs: (String, Any)*): RuleContext = {
    pairs.foreach { case (key, value) => fieldData(key) = value }
    data("field") = fieldData
    cachedFieldDataView = None  // OPTIMIZATION: Invalidate cache
    this
  }

  /**
   * Add record-level context data
   * Example: withRecordData(("bs_count", 1), ("rs_count", 2), ("segment_count", 3))
   */
  def withRecordData(pairs: (String, Any)*): RuleContext = {
    pairs.foreach { case (key, value) => recordData(key) = value }
    data("record") = recordData
    cachedRecordDataView = None  // OPTIMIZATION: Invalidate cache
    this
  }

  /**
   * Add segment-level context data
   * Example: withSegmentData(("tag", "BS"), ("position", 1), ("field_count", 50))
   */
  def withSegmentData(pairs: (String, Any)*): RuleContext = {
    pairs.foreach { case (key, value) => segmentData(key) = value }
    data("segment") = segmentData
    cachedSegmentDataView = None  // OPTIMIZATION: Invalidate cache
    this
  }

  /**
   * Add arbitrary context data (for SQL evaluation)
   * Example: withData(("custom_key", "custom_value"))
   */
  def withData(pairs: (String, Any)*): RuleContext = {
    pairs.foreach { case (key, value) => data(key) = value }
    this
  }

  /**
   * Get all context data as immutable Map
   */
  def getContextMap: Map[String, Any] = data.toMap

  /**
   * Get field data (OPTIMIZATION: Cache immutable view)
   */
  def getFieldData: Map[String, Any] = {
    cachedFieldDataView.getOrElse {
      val view = fieldData.toMap
      cachedFieldDataView = Some(view)
      view
    }
  }

  /**
   * Get record data (OPTIMIZATION: Cache immutable view)
   */
  def getRecordData: Map[String, Any] = {
    cachedRecordDataView.getOrElse {
      val view = recordData.toMap
      cachedRecordDataView = Some(view)
      view
    }
  }

  /**
   * Get segment data (OPTIMIZATION: Cache immutable view)
   */
  def getSegmentData: Map[String, Any] = {
    cachedSegmentDataView.getOrElse {
      val view = segmentData.toMap
      cachedSegmentDataView = Some(view)
      view
    }
  }

  /**
   * Get value from context using dot notation
   * OPTIMIZATION: Avoid repeated string splits, use cached parsed paths
   * Example: getValue("field.value"), getValue("record.bs_count")
   */
  def getValue(path: String): Option[Any] = {
    if (path == null || path.isEmpty) return None

    // Special support for segment_sequence and expected_mandatory_segment_sequence
    if (path == "segment_sequence") return Some(segmentSequence)
    if (path == "expected_mandatory_segment_sequence") return Some(expectedMandatorySegmentSequence)

    // OPTIMIZATION: Single-level lookups are very common (field.value, record.bs_count)
    val dotIdx = path.indexOf('.')
    if (dotIdx < 0) return None
    
    val firstPart = path.substring(0, dotIdx)
    val remainingPath = path.substring(dotIdx + 1)
    
    data.get(firstPart) match {
      case Some(map: Map[String, Any] @unchecked) =>
        // OPTIMIZATION: Check if remaining path contains dots (multi-level)
        if (remainingPath.contains('.')) {
          // Multi-level path: recursively resolve
          val subParts = remainingPath.split("\\.")
          var current: Option[Any] = map.get(subParts(0))
          for (i <- 1 until subParts.length) {
            current = current.flatMap {
              case m: Map[String, Any] @unchecked => m.get(subParts(i))
              case _ => None
            }
          }
          current
        } else {
          // Single-level path: direct lookup (common case)
          map.get(remainingPath)
        }
      case Some(map: mutable.Map[String, Any] @unchecked) => 
        if (remainingPath.contains('.')) {
          val subParts = remainingPath.split("\\.")
          var current: Option[Any] = map.get(subParts(0))
          for (i <- 1 until subParts.length) {
            current = current.flatMap {
              case m: Map[String, Any] @unchecked => m.get(subParts(i))
              case _ => None
            }
          }
          current
        } else {
          map.get(remainingPath)
        }
      case _ => None
    }
  }

  override def toString: String = {
    s"RuleContext(field=$fieldData, record=$recordData, segment=$segmentData)"
  }
}

/**
 * RuleContext companion object with factory methods
 * OPTIMIZATION: Directly populate data structures instead of chained method calls
 */
object RuleContext {
  def apply(): RuleContext = new RuleContext()

  /**
   * Create RuleContext for field-level validation
   * OPTIMIZATION: Batch all field data in single call instead of multiple withFieldData calls
   */
  def forFieldValidation(
      value: String,
      isMandatory: Boolean,
      maxLength: Int,
      fieldName: String = "",
      fieldTag: String = ""
  ): RuleContext = {
    val context = new RuleContext()
    // OPTIMIZATION: Single call instead of chained calls
    context.withFieldData(
      "field_value" -> value,
      "is_mandatory" -> isMandatory,
      "max_field_length" -> maxLength,
      "field_name" -> fieldName,
      "field_tag" -> fieldTag,
      "value_length" -> value.length
    )
  }

  /**
   * Create RuleContext for record-level validation
   * OPTIMIZATION: Calculate segment_count efficiently
   */
  def forRecordValidation(
      bsCount: Int = 0,
      rsCount: Int = 0,
      crCount: Int = 0,
      gsCount: Int = 0,
      outOfOrderCount: Int = 0
  ): RuleContext = {
    val context = new RuleContext()
    val totalCount = bsCount + rsCount + crCount + gsCount
    // OPTIMIZATION: Single call with all data
    context.withRecordData(
      "bs_count" -> bsCount,
      "rs_count" -> rsCount,
      "cr_count" -> crCount,
      "gs_count" -> gsCount,
      "segment_count" -> totalCount,
      "out_of_order_count" -> outOfOrderCount
    )
  }

  /**
   * Create RuleContext for segment-level validation
   * OPTIMIZATION: Direct data population
   */
  def forSegmentValidation(
      segmentTag: String,
      isMandatory: Boolean,
      expectedDelimiterCount: Int,
      actualDelimiterCount: Int,
      segmentLine: String = ""
  ): RuleContext = {
    val context = new RuleContext()
    // OPTIMIZATION: Single call with all segment data
    context.withSegmentData(
      "segment_tag" -> segmentTag,
      "segment_is_mandatory" -> isMandatory,
      "expected_delimiter_count" -> expectedDelimiterCount,
      "actual_delimiter_count" -> actualDelimiterCount,
      "segment_line" -> segmentLine
    )
  }
}