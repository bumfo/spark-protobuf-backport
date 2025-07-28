/*
 * Backport of Spark 3.4's ProtobufUtils to Spark 3.2.1.
 *
 * Provides helper functions for matching Catalyst and Protobuf schemas,
 * loading descriptors from Java classes or descriptor files, and converting
 * hierarchical field paths into human readable strings.  See the upstream
 * Spark implementation for a comprehensive description of the behaviour.
 */

package org.apache.spark.sql.protobuf.backport.utils

import com.google.protobuf.DescriptorProtos.{FileDescriptorProto, FileDescriptorSet}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}
import com.google.protobuf.{DescriptorProtos, Descriptors, InvalidProtocolBufferException, Message}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.protobuf.backport.shims.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

import java.io.{BufferedInputStream, FileInputStream, IOException}
import java.util.Locale
import scala.collection.JavaConverters._

/**
 * Collection of helper methods used by the Protobuf reader and writer.  In
 * addition to schema matching utilities, this object can build Protobuf
 * descriptors either from a descriptor file or a shaded Java class.  Most
 * implementations are taken verbatim from Spark 3.4 and adapted for
 * compatibility with Spark 3.2.x.
 */
private[backport] object ProtobufUtils extends Logging {

  /** Wrapper for a pair of matched fields, one Catalyst and one corresponding Protobuf field. */
  private[backport] case class ProtoMatchedField(
      catalystField: StructField,
      catalystPosition: Int,
      fieldDescriptor: FieldDescriptor)

  /**
   * Helper class to perform field lookup/matching on Protobuf schemas.
   *
   * This will match `descriptor` against `catalystSchema`, attempting to
   * find a matching field in the Protobuf descriptor for each field in the
   * Catalyst schema and vice‑versa, respecting settings for case sensitivity.
   * The match results can be accessed using the `matchedFields` property.
   *
   * @param descriptor     the descriptor in which to search for fields
   * @param catalystSchema the Catalyst schema to use for matching
   * @param protoPath      the sequence of parent field names leading to the Protobuf descriptor
   * @param catalystPath   the sequence of parent field names leading to the Catalyst schema
   */
  class ProtoSchemaHelper(
      descriptor: Descriptor,
      catalystSchema: StructType,
      protoPath: Seq[String],
      catalystPath: Seq[String]) {
    if (descriptor.getName == null) {
      throw QueryCompilationErrors.unknownProtobufMessageTypeError(
        descriptor.getName,
        descriptor.getContainingType.getName)
    }
    private[this] val protoFieldArray = descriptor.getFields.asScala.toArray
    private[this] val fieldMap = descriptor.getFields.asScala
      .groupBy(_.getName.toLowerCase(Locale.ROOT))
      .mapValues(_.toSeq)

    /** The fields which have matching equivalents in both Protobuf and Catalyst schemas. */
    val matchedFields: Seq[ProtoMatchedField] = catalystSchema.zipWithIndex.flatMap {
      case (sqlField, sqlPos) => getFieldByName(sqlField.name).map(ProtoMatchedField(sqlField, sqlPos, _))
    }

    /**
     * Validate that there are no Catalyst fields which don't have a matching Protobuf field.
     * If `ignoreNullable` is false, consider nullable Catalyst fields to be eligible to be an extra field;
     * otherwise, ignore nullable Catalyst fields when checking for extras.
     */
    def validateNoExtraCatalystFields(ignoreNullable: Boolean): Unit =
      catalystSchema.fields.foreach { sqlField =>
        if (getFieldByName(sqlField.name).isEmpty && (!ignoreNullable || !sqlField.nullable)) {
          throw QueryCompilationErrors.cannotFindCatalystTypeInProtobufSchemaError(
            toFieldStr(catalystPath :+ sqlField.name))
        }
      }

    /**
     * Validate that there are no Protobuf fields which don't have a matching Catalyst field.
     * Only required (non‑nullable) fields are checked; nullable fields are ignored.
     */
    def validateNoExtraRequiredProtoFields(): Unit = {
      val extraFields = protoFieldArray.toSet -- matchedFields.map(_.fieldDescriptor)
      extraFields.filter(_.isRequired).foreach { extraField =>
        throw QueryCompilationErrors.cannotFindProtobufFieldInCatalystError(
          toFieldStr(protoPath :+ extraField.getName))
      }
    }

    /**
     * Extract a single field from the contained Protobuf schema which has the desired field name,
     * performing the matching with proper case sensitivity according to SQLConf.resolver.
     *
     * @param name the name of the field to search for
     * @return Some(match) if a matching Protobuf field is found, otherwise None
     */
    private[backport] def getFieldByName(name: String): Option[FieldDescriptor] = {
      val candidates = fieldMap.getOrElse(name.toLowerCase(Locale.ROOT), Seq.empty)
      candidates.filter(f => SQLConf.get.resolver(f.getName, name)) match {
        case Seq(protoField) => Some(protoField)
        case Seq() => None
        case matches =>
          throw QueryCompilationErrors.protobufFieldMatchError(
            name,
            toFieldStr(protoPath),
            s"${matches.size}",
            matches.map(_.getName).mkString("[", ", ", "]"))
      }
    }
  }

  /**
   * Builds a Protobuf message descriptor either from the Java class or from a serialized descriptor
   * read from the file.  If `descFilePathOpt` is defined, the descriptor and its dependencies
   * are read from the file; otherwise `messageName` is treated as a Java class name.
   */
  def buildDescriptor(messageName: String, descFilePathOpt: Option[String]): Descriptor = {
    descFilePathOpt match {
      case Some(filePath) => buildDescriptor(descFilePath = filePath, messageName)
      case None => buildDescriptorFromJavaClass(messageName)
    }
  }

  /**
   * Loads the given Protobuf class and returns a Protobuf descriptor for it.  Ensures that the
   * class extends the shaded Protobuf Message base class.  Throws if the class cannot be found
   * or does not meet the expected requirements.
   */
  def buildDescriptorFromJavaClass(protobufClassName: String): Descriptor = {
    val shadedMessageClass = classOf[Message] // shaded in production, not in tests
    val missingShadingErrorMessage = "The jar with Protobuf classes needs to be shaded " +
      s"(com.google.protobuf.* --> ${shadedMessageClass.getPackage.getName}.*)"
    val protobufClass = try {
      Utils.classForName(protobufClassName)
    } catch {
      case e: ClassNotFoundException =>
        val explanation =
          if (protobufClassName.contains(".")) "Ensure the class is included in the jar"
          else "Ensure the class name includes package prefix"
        throw QueryCompilationErrors.protobufClassLoadError(protobufClassName, explanation, e)
      case e: NoClassDefFoundError if e.getMessage.matches("com/google/proto.*Generated.*") =>
        // This indicates the Java classes are not shaded.
        throw QueryCompilationErrors.protobufClassLoadError(protobufClassName, missingShadingErrorMessage, e)
    }
    if (!shadedMessageClass.isAssignableFrom(protobufClass)) {
      val unshadedMessageClass = Utils.classForName(
        String.join(".", "com", "google", "protobuf", "Message"))
      val explanation =
        if (unshadedMessageClass.isAssignableFrom(protobufClass)) {
          s"$protobufClassName does not extend shaded Protobuf Message class " +
            s"${shadedMessageClass.getName}. $missingShadingErrorMessage"
        } else s"$protobufClassName is not a Protobuf Message type"
      throw QueryCompilationErrors.protobufClassLoadError(protobufClassName, explanation)
    }
    val getDescriptorMethod = try {
      protobufClass.getDeclaredMethod("getDescriptor")
    } catch {
      case e: NoSuchMethodError =>
        throw QueryCompilationErrors.protobufClassLoadError(
          protobufClassName,
          "Could not find getDescriptor() method",
          e)
    }
    getDescriptorMethod.invoke(null).asInstanceOf[Descriptor]
  }

  /**
   * Find the first message descriptor that matches the name in the given descriptor file.
   */
  def buildDescriptor(descFilePath: String, messageName: String): Descriptor = {
    val descriptorOpt = parseFileDescriptorSet(descFilePath).flatMap { fileDesc =>
      fileDesc.getMessageTypes.asScala.find { desc =>
        desc.getName == messageName || desc.getFullName == messageName
      }
    }.headOption
    descriptorOpt match {
      case Some(d) => d
      case None => throw QueryCompilationErrors.unableToLocateProtobufMessageError(messageName)
    }
  }

  /**
   * Parse a descriptor file containing a serialized FileDescriptorSet and return a list of
   * FileDescriptor instances including all dependencies.
   */
  private def parseFileDescriptorSet(descFilePath: String): List[Descriptors.FileDescriptor] = {
    var fileDescriptorSet: DescriptorProtos.FileDescriptorSet = null
    try {
      val dscFile = new BufferedInputStream(new FileInputStream(descFilePath))
      fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(dscFile)
    } catch {
      case ex: InvalidProtocolBufferException =>
        throw QueryCompilationErrors.descriptorParseError(descFilePath, ex)
      case ex: IOException =>
        throw QueryCompilationErrors.cannotFindDescriptorFileError(descFilePath, ex)
    }
    try {
      val fileDescriptorProtoIndex = createDescriptorProtoMap(fileDescriptorSet)
      val fileDescriptorList: List[Descriptors.FileDescriptor] = fileDescriptorSet.getFileList.asScala.map { fileDescriptorProto =>
        buildFileDescriptor(fileDescriptorProto, fileDescriptorProtoIndex)
      }.toList
      fileDescriptorList
    } catch {
      case e: Exception =>
        throw QueryCompilationErrors.failedParsingDescriptorError(descFilePath, e)
    }
  }

  /**
   * Parse a serialized descriptor set from a byte array and return the list of
   * FileDescriptor instances including all dependencies.  This variant avoids
   * reading from the filesystem at executor runtime.  It is used when
   * the descriptor file is read on the driver and passed to executors as a
   * byte array.
   */
  private[backport] def parseFileDescriptorSet(descBytes: Array[Byte]): List[Descriptors.FileDescriptor] = {
    val fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(descBytes)
    val fileDescriptorProtoIndex = createDescriptorProtoMap(fileDescriptorSet)
    fileDescriptorSet.getFileList.asScala.map { fileDescriptorProto =>
      buildFileDescriptor(fileDescriptorProto, fileDescriptorProtoIndex)
    }.toList
  }

  /**
   * Build a Protobuf message descriptor from a serialized FileDescriptorSet.  The
   * descriptor corresponding to `messageName` is searched among all messages in
   * the descriptor set.  Throws if the message is not found.
   *
   * @param descBytes   the serialized FileDescriptorSet (as produced by protoc)
   * @param messageName the message name or fully qualified name to locate
   */
  private[backport] def buildDescriptorFromBytes(descBytes: Array[Byte], messageName: String): Descriptor = {
    val fileDescriptors = parseFileDescriptorSet(descBytes)
    val messageDescOpt = fileDescriptors.iterator.flatMap { fileDesc =>
      fileDesc.getMessageTypes.asScala.find { desc =>
        desc.getName == messageName || desc.getFullName == messageName
      }
    }.toStream.headOption
    messageDescOpt.getOrElse {
      throw QueryCompilationErrors.unableToLocateProtobufMessageError(messageName)
    }
  }

  /**
   * Recursively constructs file descriptors for all dependencies for the given FileDescriptorProto.
   */
  private def buildFileDescriptor(
      fileDescriptorProto: FileDescriptorProto,
      fileDescriptorProtoMap: Map[String, FileDescriptorProto]): Descriptors.FileDescriptor = {
    val fileDescriptorList = fileDescriptorProto.getDependencyList.asScala.map { dependency =>
      fileDescriptorProtoMap.get(dependency) match {
        case Some(dependencyProto) => buildFileDescriptor(dependencyProto, fileDescriptorProtoMap)
        case None => throw QueryCompilationErrors.protobufDescriptorDependencyError(dependency)
      }
    }
    Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, fileDescriptorList.toArray)
  }

  /**
   * Returns a map from descriptor proto name as found inside the descriptor set to protos.
   */
  private def createDescriptorProtoMap(fileDescriptorSet: FileDescriptorSet): Map[String, FileDescriptorProto] = {
    fileDescriptorSet.getFileList.asScala.map { descriptorProto =>
      descriptorProto.getName -> descriptorProto
    }.toMap
  }

  /**
   * Convert a sequence of hierarchical field names (like Seq(foo, bar)) into a human‑readable
   * string representing the field, like "field 'foo.bar'".  If `names` is empty, the string
   * "top‑level record" is returned.
   */
  private[backport] def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top‑level record"
    case n => s"field '${n.mkString(".")}'"
  }
}