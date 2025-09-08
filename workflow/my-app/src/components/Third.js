import React from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

export default function Third() {
  const navigate = useNavigate();

  return (
    <div className="third-page">
      <div className="workflow-step-3">
        <h1 className="step-heading-3">Workflow – Step 2</h1>
        <p className="step-text-3">Preprocessing & Web Scraping</p>

        <h3 className="step-heading-3">Tool:</h3>
        <p className="step-text-3">Scrapingdog API Script</p>

        <h3 className="step-heading-3">Details:</h3>
        <ul className="step-list-3">
          <li>Script dynamically adapts to the request type.</li>
          <li>
            Goal: Ensure accurate data extraction (e.g., correct company names,
            domains).
          </li>
          <li>
            Responsibility: Entire team supports refinement and validation.
          </li>
        </ul>

        {/* Arrow Button */}
        <button
          className="arrow-btn"
          onClick={() => navigate("/fourth")}
          aria-label="Go to Next Step"
        >
          ➔
        </button>
      </div>
    </div>
  );
}
