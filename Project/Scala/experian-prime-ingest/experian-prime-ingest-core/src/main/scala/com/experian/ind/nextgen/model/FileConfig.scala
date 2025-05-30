package com.experian.ind.nextgen.model

case class FileConfig(
    filename: String,
    version: String,
    format: String,
    DB_Load_ID_Generate: Boolean,
    inputFilePath: String,
    isActive: Boolean
)