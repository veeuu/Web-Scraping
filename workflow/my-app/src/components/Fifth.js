import React from "react";
import { useNavigate } from "react-router-dom";
import "../App.css";

export default function Fifth() {
  const navigate = useNavigate();

  return (
    <div className="fifth-page">
      <div className="workflow-step-5">
        <h1 className="step-heading-5">ML Development Laptop Specs</h1>

        {/* Table Section */}
        <table className="workflow-table">
          <thead>
            <tr>
              <th>Component</th>
              <th>Recommended Specification</th>
              <th>Justification for AI & ML Development</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>Processor (CPU)</td>
              <td>Intel Core Ultra 7/9 or AMD Ryzen 7/9 (latest gen)</td>
              <td>Powerful CPU keeps pipeline moving, critical for data preprocessing & automation.</td>
            </tr>
            <tr>
              <td>Memory (RAM)</td>
              <td>16 GB – 32 GB DDR5 (32 GB strongly recommended)</td>
              <td>Prevents memory errors; 32 GB is ideal for larger models & multitasking.</td>
            </tr>
            <tr>
              <td>Graphics (GPU)</td>
              <td>NVIDIA RTX 4070/4080 (8–12 GB VRAM)</td>
              <td>VRAM defines model size for fine-tuning & training locally.</td>
            </tr>
            <tr>
              <td>Storage (SSD)</td>
              <td>1 TB – 2 TB NVMe Gen4 SSD</td>
              <td>Handles huge LLM files & datasets; 2 TB avoids bottlenecks.</td>
            </tr>
            <tr>
              <td>Display</td>
              <td>15–16", QHD+ (2560×1600), 400+ nits, 99%+ sRGB</td>
              <td>Sharp visuals for dashboards, coding, & side-by-side multitasking.</td>
            </tr>
            <tr>
              <td>Connectivity</td>
              <td>Wi-Fi 6E / Wi-Fi 7, Thunderbolt 4 (USB-C)</td>
              <td>High-speed transfers for large models & fast API response.</td>
            </tr>
          </tbody>
        </table>

        {/* Requirements Section */}
        <div className="output-box">
          <h3 className="step-heading-5">Recommended Laptop Models</h3>
          <ul>
            <li>ASUS Zephyrus M16</li>
            <li>Dell XPS 15</li>
            <li>Lenovo ThinkPad P1</li>
          </ul>
        </div>

        {/* Arrow Button */}
        <button
          className="arrow-btn"
          onClick={() => navigate("/thankyou")}
          aria-label="Go to Next Step"
        >
          ➔
        </button>
      </div>
    </div>
  );
}
