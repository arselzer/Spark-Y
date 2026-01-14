import { shallowRef, watch } from 'vue'
import { EditorView, Decoration, DecorationSet } from '@codemirror/view'
import { StateField, StateEffect } from '@codemirror/state'
import { useHighlightCoordination } from './useHighlightCoordination'

/**
 * Composable for SQL-to-hypergraph highlighting in CodeMirror editors
 * Handles bidirectional highlighting between SQL text and hypergraph visualization
 */
export function useSQLHighlighting() {
  const { currentHighlight, highlightElement, clearHighlight } = useHighlightCoordination()

  // Effect for setting highlight decorations (supports multiple ranges for equivalence classes)
  const setHighlightEffect = StateEffect.define<Array<{ from: number; to: number; isPrimary: boolean }> | null>()

  // StateField for highlight decorations
  const highlightField = StateField.define<DecorationSet>({
    create() {
      return Decoration.none
    },
    update(decorations, tr) {
      decorations = decorations.map(tr.changes)
      for (const effect of tr.effects) {
        if (effect.is(setHighlightEffect)) {
          if (effect.value && effect.value.length > 0) {
            // Create decorations for all ranges (entire equivalence class)
            // Use primary highlight for the hovered attribute, secondary for others
            const ranges = effect.value.map(range => {
              const mark = Decoration.mark({
                class: range.isPrimary ? 'cm-sql-highlight' : 'cm-sql-highlight-secondary'
              })
              return mark.range(range.from, range.to)
            })
            decorations = Decoration.set(ranges)
          } else {
            decorations = Decoration.none
          }
        }
      }
      return decorations
    },
    provide: f => EditorView.decorations.from(f)
  })

  // Store the editor view for programmatic updates
  let editorView: EditorView | null = null
  let lastHighlightedElement: any = null

  // Find table aliases/names and column references in SQL with more comprehensive patterns
  function findSQLElement(text: string, pos: number): { type: 'table' | 'column', value: string, from: number, to: number } | null {
    // First try to match table WITH alias: FROM/JOIN table_name AS alias OR FROM/JOIN table_name alias
    const aliasPattern = /\b(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\b/gi

    // Check if position is within a table alias
    let match
    aliasPattern.lastIndex = 0
    while ((match = aliasPattern.exec(text)) !== null) {
      const alias = match[3]
      // Find the actual position of the alias
      const matchText = match[0]
      const aliasIndex = matchText.lastIndexOf(alias)
      const start = match.index + aliasIndex
      const end = start + alias.length

      if (pos >= start && pos <= end) {
        return { type: 'table', value: alias, from: start, to: end }
      }
    }

    // If no alias found, try to match table WITHOUT alias: FROM/JOIN table_name
    const tablePattern = /\b(FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b/gi

    tablePattern.lastIndex = 0
    while ((match = tablePattern.exec(text)) !== null) {
      const tableName = match[2]
      // Skip this match if it's part of a longer pattern with an alias
      // (already handled above)
      const afterMatch = text.substring(match.index + match[0].length, match.index + match[0].length + 20)
      if (/^\s+(?:AS\s+)?[a-zA-Z_][a-zA-Z0-9_]*\b/i.test(afterMatch)) {
        continue // This has an alias, skip it
      }

      // Find the actual position of the table name
      const matchText = match[0]
      const tableIndex = matchText.lastIndexOf(tableName)
      const start = match.index + tableIndex
      const end = start + tableName.length

      if (pos >= start && pos <= end) {
        return { type: 'table', value: tableName, from: start, to: end }
      }
    }

    // Pattern for column references: alias.column_name
    // Only match in ON, WHERE, SELECT, and other SQL clauses
    const qualifiedColumnPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b/g

    qualifiedColumnPattern.lastIndex = 0
    while ((match = qualifiedColumnPattern.exec(text)) !== null) {
      const fullReference = match[0]  // e.g., "mc1.movie_id"
      const alias = match[1]
      const columnName = match[2]
      const start = match.index
      const end = start + fullReference.length

      // Check if hover position is within this column reference
      if (pos >= start && pos <= end) {
        // Only return if this looks like a valid SQL context (not inside a string, comment, etc.)
        const before = text.substring(Math.max(0, start - 10), start)
        const after = text.substring(end, Math.min(text.length, end + 10))

        // Skip if it looks like it's in a comment or string
        if (!/--|'|"/.test(before + after)) {
          // Return the full qualified reference (alias.column) for precise matching
          return { type: 'column', value: fullReference, from: start, to: end }
        }
      }
    }

    return null
  }

  // Create CodeMirror extensions for highlighting
  const highlightingExtensions = shallowRef([
    highlightField,
    EditorView.domEventHandlers({
      mouseover(event, view) {
        const pos = view.posAtCoords({ x: event.clientX, y: event.clientY })
        if (pos !== null) {
          const text = view.state.doc.toString()
          const element = findSQLElement(text, pos)

          if (element) {
            // Only update if different from last highlighted element
            const elementKey = `${element.type}:${element.value}`
            if (lastHighlightedElement !== elementKey) {
              lastHighlightedElement = elementKey
              console.log(`SQL hover: type=${element.type}, value="${element.value}"`)

              if (element.type === 'table') {
                highlightElement({
                  type: 'edge',
                  id: element.value,
                  label: element.value
                })
              } else if (element.type === 'column') {
                highlightElement({
                  type: 'node',
                  id: element.value,
                  label: element.value
                  // Don't set hoveredAttribute - treat SQL hover like hypergraph hover
                  // All attributes in equivalence class should be highlighted the same color
                })
              }
            }
          } else if (lastHighlightedElement !== null) {
            // Clear highlight when not over any element
            lastHighlightedElement = null
            clearHighlight()
          }
        }
      },
      mouseleave(event, view) {
        // Only clear when actually leaving the editor
        lastHighlightedElement = null
        clearHighlight()
      }
    }),
    EditorView.updateListener.of((update) => {
      if (update.view && !editorView) {
        editorView = update.view
      }
    })
  ])

  // Watch for highlight changes from hypergraph to highlight SQL text
  watch(currentHighlight, (highlight) => {
    if (!editorView) {
      console.warn('EditorView not initialized for SQL highlighting')
      return
    }

    if (!highlight) {
      // Clear highlight
      editorView.dispatch({
        effects: setHighlightEffect.of(null)
      })
      return
    }

    // Find matching text in SQL
    const text = editorView.state.doc.toString()

    if (highlight.type === 'edge') {
      // Highlight table/relation: both the alias and all its column references
      const ranges: Array<{ from: number; to: number; isPrimary: boolean }> = []

      // Strip Spark's internal numbering from label
      let searchLabel = highlight.label
      if (searchLabel.includes('#')) {
        searchLabel = searchLabel.substring(0, searchLabel.indexOf('#'))
      }
      searchLabel = searchLabel.toLowerCase()

      // First try to find as an alias: FROM/JOIN table_name AS/[space] searchLabel
      const aliasPattern = new RegExp(`\\b(FROM|JOIN)\\s+[a-zA-Z_][a-zA-Z0-9_]*\\s+(?:AS\\s+)?(${searchLabel})\\b`, 'gi')

      aliasPattern.lastIndex = 0
      let match = aliasPattern.exec(text)
      if (match) {
        const alias = match[2]
        const matchText = match[0]
        const aliasIndex = matchText.lastIndexOf(alias)
        const start = match.index + aliasIndex
        const end = start + alias.length

        // Add table alias to ranges
        ranges.push({ from: start, to: end, isPrimary: true })
      } else {
        // If not found as alias, try to find as table name: FROM/JOIN searchLabel
        const tablePattern = new RegExp(`\\b(FROM|JOIN)\\s+(${searchLabel})\\b(?!\\s+(?:AS\\s+)?[a-zA-Z_][a-zA-Z0-9_]*)`, 'gi')

        tablePattern.lastIndex = 0
        match = tablePattern.exec(text)
        if (match) {
          const tableName = match[2]
          const matchText = match[0]
          const tableIndex = matchText.lastIndexOf(tableName)
          const start = match.index + tableIndex
          const end = start + tableName.length

          // Add table name to ranges
          ranges.push({ from: start, to: end, isPrimary: true })
        }
      }

      // Also highlight all column references for this table (if attributes provided)
      if (highlight.attributes && highlight.attributes.length > 0) {
        console.log(`Highlighting ${highlight.attributes.length} attributes for table ${searchLabel}:`, highlight.attributes)

        for (const attr of highlight.attributes) {
          // Strip Spark numbering from attribute
          let attrLabel = attr
          if (attrLabel.includes('#')) {
            attrLabel = attrLabel.substring(0, attrLabel.indexOf('#'))
          }

          if (!attrLabel.includes('.')) {
            continue // Skip unqualified attributes
          }

          // Extract alias and column
          const parts = attrLabel.split('.')
          const attrAlias = parts[0]
          const columnName = parts[parts.length - 1]

          // Only highlight if this attribute belongs to the current table
          if (attrAlias.toLowerCase() !== searchLabel) {
            continue
          }

          // Search for qualified references: alias.column
          const colPattern = new RegExp(`\\b(${attrAlias})\\.(${columnName})\\b`, 'gi')

          colPattern.lastIndex = 0
          let colMatch
          while ((colMatch = colPattern.exec(text)) !== null) {
            // Highlight the full qualified reference (alias.column)
            const fullMatch = colMatch[0]
            const columnStart = colMatch.index  // Start from the alias
            const columnEnd = colMatch.index + fullMatch.length

            ranges.push({
              from: columnStart,
              to: columnEnd,
              isPrimary: true  // All attributes of the table are primary (same color)
            })
          }
        }
      }

      if (ranges.length > 0) {
        // Sort ranges by position (required by CodeMirror)
        ranges.sort((a, b) => a.from - b.from)

        console.log(`Highlighting table ${searchLabel}: ${ranges.length} occurrences (1 alias + ${ranges.length - 1} column references)`)
        editorView.dispatch({
          effects: setHighlightEffect.of(ranges)
        })
      }
    } else if (highlight.type === 'node') {
      // Highlight all attributes in the equivalence class
      const attributesToHighlight = highlight.attributes || [highlight.label]
      const ranges: Array<{ from: number; to: number; isPrimary: boolean }> = []

      // Determine if we should distinguish primary from secondary
      // If hoveredAttribute exists, it means the highlight came from SQL hover (so one is special)
      // If hoveredAttribute is undefined, it came from hypergraph hover (all are equal)
      let primaryLabel: string | null = null
      if (highlight.hoveredAttribute) {
        primaryLabel = highlight.hoveredAttribute
        if (primaryLabel.includes('#')) {
          primaryLabel = primaryLabel.substring(0, primaryLabel.indexOf('#'))
        }
        primaryLabel = primaryLabel.toLowerCase()
      }

      console.log(`Highlighting equivalence class with ${attributesToHighlight.length} attributes:`, attributesToHighlight)
      console.log(`Primary attribute (SQL hover):`, primaryLabel || 'none (hypergraph hover)')

      // For each attribute in the equivalence class, find its position in SQL
      for (const attr of attributesToHighlight) {
        // Strip Spark numbering: "cn1.id#159" -> "cn1.id"
        let searchLabel = attr
        if (searchLabel.includes('#')) {
          searchLabel = searchLabel.substring(0, searchLabel.indexOf('#'))
        }

        if (!searchLabel.includes('.')) {
          continue // Skip unqualified attributes
        }

        // Check if this is the primary (hovered) attribute
        // If primaryLabel is null (hypergraph hover), all are primary
        const isPrimary = primaryLabel === null || searchLabel.toLowerCase() === primaryLabel

        // Extract alias and column for precise matching
        const parts = searchLabel.split('.')
        const alias = parts[0]
        const columnName = parts[parts.length - 1]

        // Search for exact qualified reference: alias.column
        const exactPattern = new RegExp(`\\b(${alias})\\.(${columnName})\\b`, 'gi')

        exactPattern.lastIndex = 0
        let match
        while ((match = exactPattern.exec(text)) !== null) {
          // Found a match - highlight the full qualified reference (alias.column)
          const fullMatch = match[0]  // e.g., "cn1.id"
          const columnStart = match.index  // Start from the alias
          const columnEnd = match.index + fullMatch.length

          ranges.push({
            from: columnStart,
            to: columnEnd,
            isPrimary
          })
        }
      }

      if (ranges.length > 0) {
        // Sort ranges by position (required by CodeMirror)
        ranges.sort((a, b) => a.from - b.from)

        console.log(`Highlighting ${ranges.length} occurrences in SQL (${ranges.filter(r => r.isPrimary).length} primary, ${ranges.filter(r => !r.isPrimary).length} secondary)`)
        editorView.dispatch({
          effects: setHighlightEffect.of(ranges)
        })
      }
    }
  })

  return {
    highlightingExtensions
  }
}
