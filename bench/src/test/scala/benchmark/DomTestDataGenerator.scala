package benchmark

import benchmark.DomBenchmarkProtos.{DomDocument, DomNode}
import com.google.protobuf.ByteString
import scala.collection.JavaConverters._

/**
 * Deterministic DOM tree test data generator for realistic HTML structure benchmarks.
 *
 * Creates meaningful HTML DOM trees with controlled complexity to test
 * recursive protobuf parsing performance. Uses realistic web page structures
 * including headers, navigation, articles, and nested content.
 */
object DomTestDataGenerator {

  /**
   * Create a realistic HTML document with controlled complexity.
   *
   * @param maxDepth maximum nesting depth (default: 6 for realistic web pages)
   * @param breadthFactor controls how many children nodes have (default: 3)
   * @return complete DOM document with realistic HTML structure
   */
  def createDomDocument(maxDepth: Int = 6, breadthFactor: Int = 3): DomDocument = {
    val startTime = System.currentTimeMillis()

    // Create the HTML tree structure
    val htmlRoot = createHtmlElement(0, maxDepth, breadthFactor)

    // Count total nodes for statistics
    val totalNodes = countNodes(htmlRoot)

    DomDocument.newBuilder()
      .setDoctype("<!DOCTYPE html>")
      .setCharset("UTF-8")
      .setLanguage("en")
      .setTitle("Benchmark Test Page - DOM Parsing Performance")
      .setParseTimeMs(System.currentTimeMillis() - startTime)
      .setTotalNodes(totalNodes)
      .setMaxDepth(maxDepth)
      .setRoot(htmlRoot)
      .setDescription("A realistic HTML page for testing recursive protobuf parsing performance")
      .addKeywords("protobuf")
      .addKeywords("benchmark")
      .addKeywords("spark")
      .addKeywords("performance")
      .setCanonicalUrl("https://example.com/benchmark")
      .putMetaTags("viewport", "width=device-width, initial-scale=1.0")
      .putMetaTags("author", "Spark Protobuf Benchmark")
      .putMetaTags("robots", "noindex, nofollow")
      .addStylesheets("/css/main.css")
      .addStylesheets("/css/responsive.css")
      .addScripts("/js/app.js")
      .addScripts("/js/analytics.js")
      .build()
  }

  /**
   * Create the root HTML element with realistic structure.
   */
  private def createHtmlElement(depth: Int, maxDepth: Int, breadthFactor: Int): DomNode = {
    DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("html")
      .setNodeId("html-root")
      .putAttributes("lang", "en")
      .putAttributes("dir", "ltr")
      .setDepth(depth)
      .setSiblingIndex(0)
      .addChildren(createHeadElement(depth + 1, maxDepth, breadthFactor))
      .addChildren(createBodyElement(depth + 1, maxDepth, breadthFactor))
      .build()
  }

  /**
   * Create HTML head section with meta tags, title, and links.
   */
  private def createHeadElement(depth: Int, maxDepth: Int, breadthFactor: Int): DomNode = {
    val head = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("head")
      .setNodeId("document-head")
      .setDepth(depth)
      .setSiblingIndex(0)

    // Add title
    head.addChildren(
      DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("title")
        .setDepth(depth + 1)
        .setSiblingIndex(0)
        .addChildren(createTextNode("Benchmark Test Page"))
        .build()
    )

    // Add meta tags
    head.addChildren(createMetaTag("charset", "UTF-8", depth + 1, 1))
    head.addChildren(createMetaTag("viewport", "width=device-width, initial-scale=1.0", depth + 1, 2))
    head.addChildren(createMetaTag("description", "Performance benchmark for recursive protobuf parsing", depth + 1, 3))

    // Add stylesheets
    head.addChildren(createLinkTag("stylesheet", "/css/main.css", depth + 1, 4))
    head.addChildren(createLinkTag("stylesheet", "/css/components.css", depth + 1, 5))

    head.build()
  }

  /**
   * Create HTML body with realistic page structure.
   */
  private def createBodyElement(depth: Int, maxDepth: Int, breadthFactor: Int): DomNode = {
    val body = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("body")
      .setNodeId("page-body")
      .addClassList("page")
      .addClassList("benchmark")
      .setDepth(depth)
      .setSiblingIndex(1)
      .putAttributes("data-page", "benchmark")
      .putStyles("margin", "0")
      .putStyles("padding", "0")

    var siblingIndex = 0

    // Add page header
    body.addChildren(createPageHeader(depth + 1, maxDepth, breadthFactor, siblingIndex))
    siblingIndex += 1

    // Add navigation
    body.addChildren(createNavigation(depth + 1, maxDepth, breadthFactor, siblingIndex))
    siblingIndex += 1

    // Add main content
    body.addChildren(createMainContent(depth + 1, maxDepth, breadthFactor, siblingIndex))
    siblingIndex += 1

    // Add sidebar
    body.addChildren(createSidebar(depth + 1, maxDepth, breadthFactor, siblingIndex))
    siblingIndex += 1

    // Add footer
    body.addChildren(createPageFooter(depth + 1, maxDepth, breadthFactor, siblingIndex))

    body.build()
  }

  /**
   * Create page header with navigation and branding.
   */
  private def createPageHeader(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val header = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("header")
      .setNodeId("page-header")
      .addClassList("header")
      .addClassList("main-header")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .putAttributes("role", "banner")

    // Add logo/brand
    header.addChildren(
      DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("h1")
        .setNodeId("site-title")
        .addClassList("brand")
        .setDepth(depth + 1)
        .setSiblingIndex(0)
        .addChildren(createTextNode("Spark Protobuf Benchmark"))
        .build()
    )

    header.build()
  }

  /**
   * Create navigation menu with nested lists.
   */
  private def createNavigation(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val nav = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("nav")
      .setNodeId("main-navigation")
      .addClassList("navigation")
      .addClassList("primary-nav")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .putAttributes("role", "navigation")
      .putAttributes("aria-label", "Main navigation")

    if (depth < maxDepth) {
      nav.addChildren(createNavigationList(depth + 1, maxDepth, breadthFactor))
    }

    nav.build()
  }

  /**
   * Create nested navigation list structure.
   */
  private def createNavigationList(depth: Int, maxDepth: Int, breadthFactor: Int): DomNode = {
    val ul = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("ul")
      .addClassList("nav-list")
      .addClassList(s"level-$depth")
      .setDepth(depth)
      .setSiblingIndex(0)

    val menuItems = Seq(
      ("Home", "#home"),
      ("Benchmarks", "#benchmarks"),
      ("Documentation", "#docs"),
      ("Performance", "#performance")
    )

    menuItems.zipWithIndex.foreach { case ((text, href), index) =>
      val li = DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("li")
        .addClassList("nav-item")
        .setDepth(depth + 1)
        .setSiblingIndex(index)

      // Add link
      val link = DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("a")
        .putAttributes("href", href)
        .addClassList("nav-link")
        .setDepth(depth + 2)
        .setSiblingIndex(0)
        .addChildren(createTextNode(text))

      // Add submenu for some items if we have depth remaining
      if (depth < maxDepth - 2 && (text == "Benchmarks" || text == "Documentation")) {
        link.addChildren(createSubMenu(depth + 3, maxDepth, breadthFactor, text))
      }

      li.addChildren(link.build())
      ul.addChildren(li.build())
    }

    ul.build()
  }

  /**
   * Create submenu for navigation items.
   */
  private def createSubMenu(depth: Int, maxDepth: Int, breadthFactor: Int, parentText: String): DomNode = {
    val submenu = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("ul")
      .addClassList("submenu")
      .setDepth(depth)
      .setSiblingIndex(1)

    val items = parentText match {
      case "Benchmarks" => Seq("Simple Schema", "Complex Schema", "Performance Tests")
      case "Documentation" => Seq("API Reference", "Examples", "Tutorials")
      case _ => Seq("Overview", "Details")
    }

    items.zipWithIndex.foreach { case (text, index) =>
      val li = DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("li")
        .addClassList("submenu-item")
        .setDepth(depth + 1)
        .setSiblingIndex(index)
        .addChildren(
          DomNode.newBuilder()
            .setNodeType(DomNode.NodeType.ELEMENT_NODE)
            .setTagName("a")
            .putAttributes("href", s"#${text.toLowerCase.replace(" ", "-")}")
            .addClassList("submenu-link")
            .setDepth(depth + 2)
            .setSiblingIndex(0)
            .addChildren(createTextNode(text))
            .build()
        )

      submenu.addChildren(li.build())
    }

    submenu.build()
  }

  /**
   * Create main content area with articles and sections.
   */
  private def createMainContent(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val main = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("main")
      .setNodeId("main-content")
      .addClassList("content")
      .addClassList("primary-content")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .putAttributes("role", "main")

    var childIndex = 0

    // Add article sections
    main.addChildren(createArticle("Introduction", depth + 1, maxDepth, breadthFactor, childIndex))
    childIndex += 1

    main.addChildren(createArticle("Performance Results", depth + 1, maxDepth, breadthFactor, childIndex))
    childIndex += 1

    if (depth < maxDepth - 2) {
      main.addChildren(createDataSection(depth + 1, maxDepth, breadthFactor, childIndex))
    }

    main.build()
  }

  /**
   * Create article with nested content.
   */
  private def createArticle(title: String, depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val article = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("article")
      .setNodeId(s"article-${title.toLowerCase.replace(" ", "-")}")
      .addClassList("article")
      .addClassList("content-section")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)

    // Add header
    article.addChildren(
      DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("h2")
        .addClassList("section-title")
        .setDepth(depth + 1)
        .setSiblingIndex(0)
        .addChildren(createTextNode(title))
        .build()
    )

    // Add paragraphs
    val paragraphs = title match {
      case "Introduction" => Seq(
        "This benchmark tests the performance of recursive protobuf parsing using realistic DOM tree structures.",
        "The test cases include various levels of nesting and different node types to simulate real-world HTML documents.",
        "Performance is measured across different parser implementations including generated code and direct wire format parsing."
      )
      case "Performance Results" => Seq(
        "Generated parsers consistently outperform dynamic message parsers by 10-15x in most test scenarios.",
        "Wire format parsers provide a good middle ground with 2-3x better performance than dynamic parsers.",
        "Memory usage scales linearly with tree depth due to efficient buffer sharing in nested conversions."
      )
      case _ => Seq(
        "This section contains detailed information about the topic.",
        "Multiple paragraphs help test text node parsing performance."
      )
    }

    paragraphs.zipWithIndex.foreach { case (text, index) =>
      article.addChildren(createParagraph(text, depth + 1, index + 1))
    }

    // Add nested list if we have depth remaining
    if (depth < maxDepth - 2) {
      article.addChildren(createContentList(depth + 1, maxDepth, breadthFactor, paragraphs.length + 1))
    }

    article.build()
  }

  /**
   * Create paragraph with text content.
   */
  private def createParagraph(text: String, depth: Int, siblingIndex: Int): DomNode = {
    DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("p")
      .addClassList("paragraph")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .addChildren(createTextNode(text))
      .build()
  }

  /**
   * Create nested content list.
   */
  private def createContentList(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val ul = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("ul")
      .addClassList("content-list")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)

    val items = Seq(
      "High performance protobuf parsing",
      "Recursive message structure support",
      "Memory efficient buffer sharing",
      "Multiple parser implementation strategies"
    )

    items.zipWithIndex.foreach { case (text, index) =>
      val li = DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("li")
        .addClassList("list-item")
        .setDepth(depth + 1)
        .setSiblingIndex(index)
        .addChildren(createTextNode(text))

      // Add nested list occasionally for more complexity
      if (depth < maxDepth - 2 && index == 1) {
        li.addChildren(createNestedFeatureList(depth + 2, maxDepth, breadthFactor))
      }

      ul.addChildren(li.build())
    }

    ul.build()
  }

  /**
   * Create nested feature list.
   */
  private def createNestedFeatureList(depth: Int, maxDepth: Int, breadthFactor: Int): DomNode = {
    val ul = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("ul")
      .addClassList("nested-list")
      .setDepth(depth)
      .setSiblingIndex(1)

    val features = Seq("Generated code optimization", "Wire format parsing", "Dynamic message fallback")

    features.zipWithIndex.foreach { case (text, index) =>
      ul.addChildren(
        DomNode.newBuilder()
          .setNodeType(DomNode.NodeType.ELEMENT_NODE)
          .setTagName("li")
          .addClassList("nested-item")
          .setDepth(depth + 1)
          .setSiblingIndex(index)
          .addChildren(createTextNode(text))
          .build()
      )
    }

    ul.build()
  }

  /**
   * Create data section with tables and structured content.
   */
  private def createDataSection(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val section = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("section")
      .setNodeId("data-section")
      .addClassList("data-section")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)

    // Add section header
    section.addChildren(
      DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("h3")
        .addClassList("subsection-title")
        .setDepth(depth + 1)
        .setSiblingIndex(0)
        .addChildren(createTextNode("Benchmark Data"))
        .build()
    )

    // Add data table if we have depth
    if (depth < maxDepth - 2) {
      section.addChildren(createDataTable(depth + 1, maxDepth, breadthFactor, 1))
    }

    section.build()
  }

  /**
   * Create data table with performance metrics.
   */
  private def createDataTable(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val table = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("table")
      .addClassList("data-table")
      .addClassList("performance-metrics")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .putAttributes("border", "1")

    // Add table header
    val thead = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("thead")
      .setDepth(depth + 1)
      .setSiblingIndex(0)

    val headerRow = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("tr")
      .setDepth(depth + 2)
      .setSiblingIndex(0)

    val headers = Seq("Parser Type", "Avg Time (ns)", "Throughput", "Memory Usage")
    headers.zipWithIndex.foreach { case (text, index) =>
      headerRow.addChildren(
        DomNode.newBuilder()
          .setNodeType(DomNode.NodeType.ELEMENT_NODE)
          .setTagName("th")
          .addClassList("table-header")
          .setDepth(depth + 3)
          .setSiblingIndex(index)
          .addChildren(createTextNode(text))
          .build()
      )
    }

    thead.addChildren(headerRow.build())
    table.addChildren(thead.build())

    // Add table body with data rows
    val tbody = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("tbody")
      .setDepth(depth + 1)
      .setSiblingIndex(1)

    val testData = Seq(
      ("Generated Parser", "1,649", "High", "Low"),
      ("Wire Format Parser", "2,442", "Medium", "Medium"),
      ("Dynamic Parser", "24,992", "Low", "High")
    )

    testData.zipWithIndex.foreach { case ((parser, time, throughput, memory), rowIndex) =>
      val row = DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("tr")
        .addClassList("data-row")
        .setDepth(depth + 2)
        .setSiblingIndex(rowIndex)

      val values = Seq(parser, time, throughput, memory)
      values.zipWithIndex.foreach { case (value, colIndex) =>
        row.addChildren(
          DomNode.newBuilder()
            .setNodeType(DomNode.NodeType.ELEMENT_NODE)
            .setTagName("td")
            .addClassList("table-cell")
            .setDepth(depth + 3)
            .setSiblingIndex(colIndex)
            .addChildren(createTextNode(value))
            .build()
        )
      }

      tbody.addChildren(row.build())
    }

    table.addChildren(tbody.build())
    table.build()
  }

  /**
   * Create sidebar with additional content.
   */
  private def createSidebar(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val aside = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("aside")
      .setNodeId("sidebar")
      .addClassList("sidebar")
      .addClassList("secondary-content")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .putAttributes("role", "complementary")

    // Add sidebar title
    aside.addChildren(
      DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("h3")
        .addClassList("sidebar-title")
        .setDepth(depth + 1)
        .setSiblingIndex(0)
        .addChildren(createTextNode("Related Information"))
        .build()
    )

    // Add sidebar links
    if (depth < maxDepth - 1) {
      aside.addChildren(createSidebarLinks(depth + 1, maxDepth, breadthFactor, 1))
    }

    aside.build()
  }

  /**
   * Create sidebar links list.
   */
  private def createSidebarLinks(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val ul = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("ul")
      .addClassList("sidebar-links")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)

    val links = Seq(
      ("Apache Spark Documentation", "https://spark.apache.org/docs/"),
      ("Protocol Buffers Guide", "https://developers.google.com/protocol-buffers/"),
      ("Performance Tuning Tips", "#performance-tips"),
      ("Source Code Repository", "#source-code")
    )

    links.zipWithIndex.foreach { case ((text, href), index) =>
      ul.addChildren(
        DomNode.newBuilder()
          .setNodeType(DomNode.NodeType.ELEMENT_NODE)
          .setTagName("li")
          .addClassList("sidebar-item")
          .setDepth(depth + 1)
          .setSiblingIndex(index)
          .addChildren(
            DomNode.newBuilder()
              .setNodeType(DomNode.NodeType.ELEMENT_NODE)
              .setTagName("a")
              .putAttributes("href", href)
              .addClassList("sidebar-link")
              .setDepth(depth + 2)
              .setSiblingIndex(0)
              .addChildren(createTextNode(text))
              .build()
          )
          .build()
      )
    }

    ul.build()
  }

  /**
   * Create page footer.
   */
  private def createPageFooter(depth: Int, maxDepth: Int, breadthFactor: Int, siblingIndex: Int): DomNode = {
    val footer = DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("footer")
      .setNodeId("page-footer")
      .addClassList("footer")
      .addClassList("site-footer")
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .putAttributes("role", "contentinfo")

    // Add footer content
    footer.addChildren(
      DomNode.newBuilder()
        .setNodeType(DomNode.NodeType.ELEMENT_NODE)
        .setTagName("p")
        .addClassList("footer-text")
        .setDepth(depth + 1)
        .setSiblingIndex(0)
        .addChildren(createTextNode("© 2024 Spark Protobuf Benchmark. Generated for performance testing."))
        .build()
    )

    footer.build()
  }

  // Helper methods

  private def createTextNode(content: String): DomNode = {
    DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.TEXT_NODE)
      .setTextContent(content)
      .build()
  }

  private def createMetaTag(name: String, content: String, depth: Int, siblingIndex: Int): DomNode = {
    DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("meta")
      .putAttributes("name", name)
      .putAttributes("content", content)
      .setIsVoidElement(true)
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .build()
  }

  private def createLinkTag(rel: String, href: String, depth: Int, siblingIndex: Int): DomNode = {
    DomNode.newBuilder()
      .setNodeType(DomNode.NodeType.ELEMENT_NODE)
      .setTagName("link")
      .putAttributes("rel", rel)
      .putAttributes("href", href)
      .setIsVoidElement(true)
      .setDepth(depth)
      .setSiblingIndex(siblingIndex)
      .build()
  }

  private def countNodes(node: DomNode): Int = {
    1 + node.getChildrenList.asScala.map(countNodes _).sum
  }

  /**
   * Get deterministic binary data for DOM document.
   */
  def getDomBinary(maxDepth: Int = 6, breadthFactor: Int = 3): Array[Byte] = {
    createDomDocument(maxDepth, breadthFactor).toByteArray
  }

  /**
   * Create simple DOM tree for basic testing.
   */
  def createSimpleDomDocument(): DomDocument = createDomDocument(maxDepth = 3, breadthFactor = 2)

  /**
   * Create complex DOM tree for stress testing.
   */
  def createComplexDomDocument(): DomDocument = createDomDocument(maxDepth = 8, breadthFactor = 4)
}