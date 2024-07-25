<!--
  Normalize document for comparison:
  - Standard indentation.
  - Strip non-significant spaces.
  - Sort all tags and attributes.
-->
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output indent="yes"/>
    <xsl:strip-space elements="*"/>

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="*[not(text())]">
        <xsl:copy>
            <xsl:apply-templates select="@*">
                <xsl:sort select="local-name()"/>
            </xsl:apply-templates>
            <xsl:apply-templates select="node()">
                <xsl:sort select="local-name()"/>
            </xsl:apply-templates>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
