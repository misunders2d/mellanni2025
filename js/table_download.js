
// brand tailored promotions
function extractBrandTailoredPromotions() {
    const tableRows = document.querySelectorAll('kat-table-row') // get all table rows from the screen

    let allRows = []
    tableRows.forEach((row, index) => {
        if (index === 0) return
        let rowData = []
        const divColumnNames = ["promoSchedules", "promoTotalSales"]
        let rowCells = row.querySelectorAll('kat-table-cell')
        rowCells.forEach((cell) => {
            let cellName = cell.getAttribute('data-testid') || 'noname'
            if (cellName === "audienceName") {
                let value1 = cell.querySelector('div').innerText
                let value2 = cell.querySelector('span').innerText

                rowData.push(value1, value2)
            } else if (divColumnNames.includes(cellName)) {
                let divsEl = cell.querySelectorAll('div')
                let value1 = divsEl[0].innerText
                let value2 = divsEl[1].innerText

                rowData.push(value1, value2)
            } else {
                let value1 = cell.innerText
                rowData.push(value1)
            }
        })
        allRows.push(rowData)
    })
    return allRows
}

allRows = extractBrandTailoredPromotions()
const brandHeader = ['Brand', 'Promo Name', 'Audience', 'Size at creation', 'Status', 'Start date','End date', 'Discount', 'Redemptions', 'Sales', 'Spend', 'Actions']

function createCSV(data, header, fileName = 'data.csv', separator = ',') {
    const csvHeader = header.join(separator)
    const csvRows = allRows.map(row => row.join(separator)).join('\n')
    const csvContent = `${csvHeader}\n${csvRows}`

    // Step 3: Create and download the CSV file
    const blob = new Blob([csvContent], { type: 'text/csvcharset=utf-8' })
    const link = document.createElement('a')
    const url = URL.createObjectURL(blob)
    link.setAttribute('href', url)
    link.setAttribute('download', fileName)
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    URL.revokeObjectURL(url)
}

// creator connections



//download brand tailored
createCSV(data = allRows, header = brandHeader, fileName = 'brand_tailored.csv', separator = '\t')