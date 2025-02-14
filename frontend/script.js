function onImageLoad() {
  console.log("Image has loaded!");

  // Example: Change the background color of the body after the image loads
  document.body.style.backgroundColor = "#f0f0f0";

  // You can add any other actions you want to perform after the image has loaded here.
}

// document.addEventListener("DOMContentLoaded", function () {
//   const searchResults = document.getElementById("searchResults");

//   // placeholder data - this would be replaced with real search results
//   const placeholderResults = [
//     {
//       title: "Example Result 1",
//       url: "https://example.com/1",
//       description: "This is a placeholder description for the first result.",
//     },
//     {
//       title: "Example Result 2",
//       url: "https://example.com/2",
//       description: "This is a placeholder description for the second result.",
//     },
//     // ...more placeholder results
//     {
//       title: "Example Result 3",
//       url: "https://example.com/3",
//       description: "This is a placeholder description for the first result.",
//     },
//     {
//       title: "Example Result 4",
//       url: "https://example.com/4",
//       description: "This is a placeholder description for the second result.",
//     },
//     {
//       title: "Example Result 5",
//       url: "https://example.com/5",
//       description: "This is a placeholder description for the first result.",
//     },
//     {
//       title: "Example Result 6",
//       url: "https://example.com/6",
//       description: "This is a placeholder description for the second result.",
//     },
//     {
//       title: "Example Result 3",
//       url: "https://example.com/3",
//       description: "This is a placeholder description for the first result.",
//     },
//     {
//       title: "Example Result 4",
//       url: "https://example.com/4",
//       description: "This is a placeholder description for the second result.",
//     },
//     {
//       title: "Example Result 5",
//       url: "https://example.com/5",
//       description: "This is a placeholder description for the first result.",
//     },
//     {
//       title: "Example Result 6",
//       url: "https://example.com/6",
//       description: "This is a placeholder description for the second result.",
//     },
//   ];

//   // Function to create a result item
//   function createResultItem(result) {
//     const container = document.createElement("div");
//     container.classList.add("resultItem");

//     const title = document.createElement("h3");
//     const link = document.createElement("a");
//     link.href = result.url;
//     link.textContent = result.title;
//     title.appendChild(link);
//     container.appendChild(title);

//     const url = document.createElement("p");
//     url.textContent = result.url;
//     container.appendChild(url);

//     const description = document.createElement("p");
//     description.textContent = result.description;
//     container.appendChild(description);

//     return container;
//   }

//   // Insert the placeholder results into the page
//   placeholderResults.forEach((result) => {
//     const resultItem = createResultItem(result);
//     searchResults.appendChild(resultItem);
//   });
// });

function performSearch() {
  const query = document.getElementById("searchInput").value;
  fetch(`/search?query=${encodeURIComponent(query)}`)
    .then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return response.json();
    })
    .then((data) => {
      // Use the displaySearchResults function to show the results
      displaySearchResults(data);
    })
    .catch((error) => {
      console.error("Error fetching the search results:", error);
    });
}

function displaySearchResults(results) {
  const searchResults = document.getElementById("searchResults");
  if (searchResults) {
    searchResults.innerHTML = ""; // Clear previous results

    results.forEach((result) => {
      const resultItem = createResultItem(result);
      searchResults.appendChild(resultItem);
    });
  } else {
    // If searchResults is null, log an error or handle it appropriately
    console.error("The searchResults container was not found.");
  }
}

// Function to create a result item
function createResultItem(result) {
  const container = document.createElement("div");
  container.classList.add("resultItem");

  const title = document.createElement("h3");
  const link = document.createElement("a");
  link.href = result.url;
  link.textContent = result.title || result.url; // Use the title or the URL if title is not available
  title.appendChild(link);
  container.appendChild(title);

  const url = document.createElement("p");
  url.textContent = result.url;
  container.appendChild(url);

  const description = document.createElement("p");
  description.textContent = result.description;
  container.appendChild(description);

  return container;
}

// Bind the performSearch function to the form submission event
document.addEventListener("DOMContentLoaded", function () {
  const searchForm = document.querySelector(".search-form");
  searchForm.addEventListener("submit", function (event) {
    event.preventDefault(); // Prevent the form from submitting the traditional way
    performSearch();
  });
});

document.addEventListener("DOMContentLoaded", function () {
  const searchInput = document.getElementById("searchInput");
  const resultsContainer = document.getElementById("autocompleteResults");

  searchInput.addEventListener("input", function (e) {
    // Your logic to fetch autocomplete suggestions goes here
    // For demonstration, let's use static data
    const suggestions = ["abc", "abcd", "bcdefdg"];
    const inputValue = e.target.value;

    // Clear previous results
    resultsContainer.innerHTML = "";
    resultsContainer.style.display = "none";

    // Filter suggestions based on input and only display if there's input
    if (inputValue) {
      suggestions
        .filter(function (item) {
          return item.toLowerCase().startsWith(inputValue.toLowerCase());
        })
        .forEach(function (suggested) {
          const div = document.createElement("div");
          div.textContent = suggested;
          div.className = "suggestion";
          div.addEventListener("click", function () {
            searchInput.value = suggested; // update the search input with the selected term
            resultsContainer.style.display = "none"; // hide the suggestions
          });
          resultsContainer.appendChild(div);
        });

      if (resultsContainer.childElementCount > 0) {
        resultsContainer.style.display = "block"; // Show suggestions if there are any
      }
    }
  });

  // hide suggestions when clicking outside
  document.addEventListener("click", function (e) {
    if (e.target !== searchInput) {
      resultsContainer.style.display = "none";
    }
  });
});

document.addEventListener("DOMContentLoaded", function () {
  const searchButton = document.getElementById("searchButton");

  searchButton.addEventListener("mouseover", function () {
    searchButton.textContent = "GO";
  });

  searchButton.addEventListener("mouseout", function () {
    searchButton.textContent = "Search";
  });
});
