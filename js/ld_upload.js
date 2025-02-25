
function getNumberOfPages() {
    // extracts max number of pages from the original deal layout
    const maxNumber = document.querySelector('form[name="pageNumber"] input[name="pageNumber"]').max
    return Number(maxNumber)
}

function setPageSize(maxNumber) {
    // function to update page size to 500 if number of pages is more than 1
    if (maxNumber > 1) {
        const parsedUrl = new URL( document.URL )
        parsedUrl.searchParams.set('pageSize','500')
        window.location.href(parsedUrl.toString())
    }
}

