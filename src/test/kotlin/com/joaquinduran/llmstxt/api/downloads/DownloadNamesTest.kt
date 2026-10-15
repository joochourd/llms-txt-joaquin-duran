package com.joaquinduran.llmstxt.api.downloads

import com.joaquinduran.llmstxt.api.downloads.DownloadNames
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DownloadNamesTest {
    @Test
    fun `root and path names identify the submitted website and historical version`() {
        assertEquals("example.com.txt", DownloadNames.text("https://example.com/"))
        assertEquals("example.com-docs-guide.txt", DownloadNames.text("https://example.com/docs/guide/"))
        assertEquals("example.com-docs-v3.txt", DownloadNames.text("https://example.com/docs", 3))
    }

    @Test
    fun `untrusted URL characters cannot become paths or header control characters`() {
        val name = DownloadNames.text("https://example.com/caf%C3%A9/%22%0D%0Aevil%5Cfile")
        assertEquals("example.com-cafe-evil-file.txt", name)
        assertTrue(name.matches(Regex("[a-z0-9._-]+\\.txt")))
        assertFalse(name.contains('/'))
    }

    @Test
    fun `queries and truncated paths retain stable distinguishing suffixes`() {
        val query = DownloadNames.text("https://example.com/docs?lang=en")
        assertEquals(query, DownloadNames.text("https://example.com/docs?lang=en"))
        assertNotEquals(query, DownloadNames.text("https://example.com/docs?lang=es"))
        assertTrue(query.startsWith("example.com-docs-"))
        val prefix = "https://example.com/" + "a".repeat(200)
        assertTrue(DownloadNames.text(prefix).length < 150)
        assertNotEquals(DownloadNames.text(prefix), DownloadNames.text(prefix + "b"))
    }

    @Test
    fun `duplicate rows and colliding natural suffixes always have distinct archive entries`() {
        val used = mutableSetOf<String>()
        assertEquals("example.com.txt", DownloadNames.unique("example.com.txt", used))
        assertEquals("example.com-2.txt", DownloadNames.unique("example.com.txt", used))
        assertEquals("example.com-2-2.txt", DownloadNames.unique("example.com-2.txt", used))
        assertEquals("example.com-3.txt", DownloadNames.unique("example.com.txt", used))
    }
}
