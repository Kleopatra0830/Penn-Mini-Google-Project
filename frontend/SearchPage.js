// Function to fetch and store the wordlist
function fetchAndStoreWordlist() {
  fetch("/wordlist")
    .then((response) => {
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return response.json();
    })
    .then((data) => {
      // Store the wordlist in localStorage
      localStorage.setItem("wordlist", JSON.stringify(data));
    })
    .catch((error) => {
      console.error("Error fetching wordlist:", error);
    });
}

document.addEventListener("DOMContentLoaded", function () {
  fetchAndStoreWordlist();
});

function performSearch() {
  const query = document.getElementById("searchInput").value;
  // Store the query in localStorage
  localStorage.setItem("searchQuery", query);
  // Redirect to the search results page
  window.location.href = "SearchResults.html";
}

function onImageLoad() {
  console.log("Image has loaded!");

  // Example: Change the background color of the body after the image loads
  document.body.style.backgroundColor = "#f0f0f0";

  // You can add any other actions you want to perform after the image has loaded here.
}

document.addEventListener("DOMContentLoaded", function () {
  const searchInput = document.getElementById("searchInput");
  const resultsContainer = document.getElementById("autocompleteResults");

  const searchForm = document.querySelector(".search-form");
  searchForm.addEventListener("submit", function (event) {
    event.preventDefault(); // Prevent the form from submitting the traditional way
    performSearch();
  });

  searchInput.addEventListener("input", function (e) {
    // Your logic to fetch autocomplete suggestions goes here
    const suggestions = JSON.parse(localStorage.getItem("wordlist")) || [];
    console.log(suggestions);
    // For demonstration, let's use static data
    // const suggestions = ["abc", "abcd", "bcdefdg"];
    const inputValue = e.target.value;

    // Clear previous results
    resultsContainer.innerHTML = "";
    resultsContainer.style.display = "none";

    // Filter suggestions based on input and only display if there's input
    if (inputValue) {
      suggestions[0]
        .filter(function (item) {
          return item.toLowerCase().startsWith(inputValue.toLowerCase());
        })
        .slice(0, 10)
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
      } else {
        const suggestion = spellcheck(inputValue, suggestions[0]);

        if (suggestion && levenshteinDistance(inputValue, suggestion) <= 2) {
          // Threshold of 2 edits
          console.log("Did you mean:", suggestion);
          // You can also display this suggestion in the UI
          const div = document.createElement("div");
          div.textContent = suggestion;
          div.className = "suggestion";
          div.addEventListener("click", function () {
            searchInput.value = suggestion; // update the search input with the selected term
            resultsContainer.style.display = "none"; // hide the suggestions
          });
          resultsContainer.appendChild(div);
          resultsContainer.style.display = "block";
        }
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

function levenshteinDistance(a, b) {
  const matrix = [];

  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i];
  }

  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) == a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          Math.min(matrix[i][j - 1] + 1, matrix[i - 1][j] + 1)
        );
      }
    }
  }

  return matrix[b.length][a.length];
}

function spellcheck(input, dictionary) {
  let minDistance = Infinity;
  let closestWord = "";

  for (const word of dictionary) {
    const distance = levenshteinDistance(input, word);
    if (distance < minDistance) {
      minDistance = distance;
      closestWord = word;
    }
  }

  return closestWord;
}
