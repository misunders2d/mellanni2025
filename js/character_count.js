function onOpen() {
    DocumentApp.getUi().createMenu('Character Count')
        .addItem('Recalculate Character Count', 'calculateCharacters')
        .addItem('Show Character Count Sidebar', 'showSidebar')
        .addItem('Remove Character Count', 'removeCount')
        .addToUi();
  }
  
  function removeCount() {
    const doc = DocumentApp.getActiveDocument();
    const body = doc.getBody();
    const paragraphs = body.getParagraphs();
    const initialParagraphCount = paragraphs.length;
    for (let i = paragraphs.length - 1; i >= 0; i--) {
      const paragraph = paragraphs[i];
      const text = paragraph.getText().trim();
      if (text.startsWith("Character count:")) {
        try {
          body.removeChild(paragraph); // Try to remove the paragraph
          } catch (error) {
            paragraph.setText(" "); // If removal fails, clear the text
          }
      }
    }
  }

  function calculateCharacters() {
    const doc = DocumentApp.getActiveDocument();
    const body = doc.getBody();
    const paragraphs = body.getParagraphs();
    let totalCount = 0;
    const style = {};
    style[DocumentApp.Attribute.BOLD] = true;
    style[DocumentApp.Attribute.FONT_SIZE] = 8;
    style[DocumentApp.Attribute.UNDERLINE] = true;
    
    // Loop through each paragraph in reverse order
    for (let i = paragraphs.length - 1; i >= 0; i--) {
      const paragraph = paragraphs[i];
      const text = paragraph.getText().trim();
      const charCount = text.length; // Number of characters
      if (text.startsWith("Character count:")) {
        body.removeChild(paragraphs[i]);
      } else if (charCount > 0) {
        totalCount += charCount;
        const countParagraph = body.insertParagraph(i + 1, `Character count: ${charCount}`);
        countParagraph.setAttributes(style);
      }
    }
    const totalParagraph = body.insertParagraph(paragraphs.length + 3, `Character count: Total ${totalCount}`);
    totalParagraph.setAttributes(style);
  }
  
const buttonStyle = "border: none; padding:10px; border-radius:5px; background-color:limegreen;"
const bodyStyle = "font-family:Arial, sans-serif; font-size: 16px;"
function showSidebar() {
  const html = HtmlService.createHtmlOutput(`
    <html>
      <body style="${bodyStyle}">
        <p>Selected text character count: <span id="charCount" style="font-weight:bold;">0</span></p>
        <button style="${buttonStyle}"
          onmouseover="this.style.backgroundColor='green'"
          onmouseout="this.style.backgroundColor='limegreen'"
          onclick="google.script.run.updateCharCount()">Recalculate
        </button>
      </body>
    </html>
  `)
    .setTitle('Character Count')
    .setWidth(300)
    .setHeight(100);
  DocumentApp.getUi().showSidebar(html);
}

function updateCharCount() {
  const doc = DocumentApp.getActiveDocument();
  const selection = doc.getSelection();
  let charCount = 0;

  if (selection) {
    const elements = selection.getRangeElements();
    for (let i = 0; i < elements.length; i++) {
      const element = elements[i].getElement();
      if (element.editAsText) {
        const text = element.asText().getText();
        const start = elements[i].getStartOffset();
        const end = elements[i].getEndOffsetInclusive();
        // Handle partial selection
        if (start >= 0 && end >= 0) {
          const selectedText = text.substring(start, end + 1);
          charCount += selectedText.length;
        } else {
          charCount += text.length; // Full element selected
        }
      }
    }
  }

  const ui = DocumentApp.getUi();
  const html = HtmlService.createHtmlOutput(`
    <html>
      <body style="${bodyStyle}">
        <p>Selected text character count: <span id="charCount" style="font-weight:bold;">${charCount}</span></p>
        <button style="${buttonStyle}"
          onmouseover="this.style.backgroundColor='green'"
          onmouseout="this.style.backgroundColor='limegreen'"
          onclick="google.script.run.updateCharCount()">Recalculate
        </button>
      </body>
    </html>
  `);
  ui.showSidebar(html);
}