import React from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

export default function Seventh() {
  const navigate = useNavigate();

  return (
    <div className="seventh-page">
      <div className="workflow-step-7">
        <h1 className="step-heading-7">Workflow – Step 6</h1>
        <p className="step-text-7">File Handling & Request Management</p>

        <h3 className="step-heading-7">Responsibility:</h3>
        <p className="step-text-7">Vaishnavi and Swapnil</p>

        <h3 className="step-heading-7">Tasks:</h3>
        <ul className="step-list-7">
          <li>Manage incoming files from the technographics team</li>
          <li>Pre-integrate validated requests into web scraping pipeline</li>
          <li>Actively support API Gateway use case development</li>
        </ul>
      </div>

      {/* Arrow Button */}
      <button
        className="arrow-btn"
        onClick={() => navigate("/eighth")}
        aria-label="Go to Next Step"
      >
        ➔
      </button>
    </div>
  );
}
