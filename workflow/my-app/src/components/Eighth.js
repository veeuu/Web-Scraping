import React from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

export default function Eighth() {
  const navigate = useNavigate();

  return (
    <div className="eighth-page">
      <div className="workflow-step-8">
        <h1 className="step-heading-8">Workflow – Step 7</h1>
        <p className="step-text-8">Innovation & Model Exploration</p>

        <h3 className="step-heading-8">Responsibility:</h3>
        <p className="step-text-8">Full Team</p>

        <h3 className="step-heading-8">Tasks:</h3>
        <ul className="step-list-8">
          <li>Evaluate alternative APIs and models</li>
          <li>Optimize prompts and context chunking</li>
          <li>Study cost-performance trade-offs</li>
          <li>Experiment with multi-model orchestration</li>
        </ul>
      </div>

      {/* Arrow Button */}
      <button
        className="arrow-btn"
        onClick={() => navigate("/summary")}
        aria-label="Go to Summary"
      >
        ➔
      </button>
    </div>
  );
}
