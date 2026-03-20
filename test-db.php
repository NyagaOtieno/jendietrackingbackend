<?php
// Turn off warnings and handle errors manually
mysqli_report(MYSQLI_REPORT_OFF);

// Initialize connection
$conn1 = @mysqli_connect("18.218.110.222", "root", "nairobiyetu", "uradi");

// Check connection
if (!$conn1) {
    die("❌ Connection Failed: " . mysqli_connect_error());
}

// Optional: set charset (best practice)
mysqli_set_charset($conn1, "utf8mb4");

// Success message
echo "✅ Connected successfully";

// Close connection
mysqli_close($conn1);
?>