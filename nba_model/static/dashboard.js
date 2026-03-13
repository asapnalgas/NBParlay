// NBA Self-Learning System - Dashboard JavaScript

// Global variables
let accuracyChart = null;
let errorChart = null;
let refreshInterval = null;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log('🎯 Dashboard initializing...');
    
    // Initialize charts
    initCharts();
    
    // Load initial data
    loadSystemStatus();
    loadSystemInfo();
    loadLearningProgress();
    loadMetrics();
    
    // Set up auto-refresh (every 10 seconds)
    refreshInterval = setInterval(function() {
        loadSystemStatus();
        loadLearningProgress();
        loadMetrics();
        updateLastUpdate();
    }, 10000);
    
    console.log('✓ Dashboard ready');
});

// Initialize charts
function initCharts() {
    // Accuracy Trend Chart
    const accuracyCtx = document.getElementById('accuracyChart');
    if (accuracyCtx) {
        accuracyChart = new Chart(accuracyCtx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Accuracy %',
                    data: [],
                    borderColor: '#2ca02c',
                    backgroundColor: 'rgba(44, 160, 44, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 4,
                    pointBackgroundColor: '#2ca02c',
                    pointBorderColor: '#fff',
                    pointBorderWidth: 2,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        labels: {
                            font: { size: 12 },
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        max: 100,
                        ticks: {
                            callback: function(value) {
                                return value + '%';
                            }
                        }
                    }
                }
            }
        });
    }

    // Error Analysis Chart
    const errorCtx = document.getElementById('errorChart');
    if (errorCtx) {
        errorChart = new Chart(errorCtx, {
            type: 'bar',
            data: {
                labels: [],
                datasets: [{
                    label: 'MAE (Mean Absolute Error)',
                    data: [],
                    backgroundColor: '#ff7f0e',
                    borderColor: '#ff7f0e',
                    borderWidth: 1,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        labels: {
                            font: { size: 12 },
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                    }
                }
            }
        });
    }
}

// Load system status
async function loadSystemStatus() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();

        if (data.status === 'success') {
            const orchestrator = data.orchestrator;
            const isRunning = data.system.is_running;

            // Update status badge
            const statusBadge = document.querySelector('.status-indicator');
            const statusText = document.querySelector('.status-text');
            
            if (isRunning) {
                statusBadge.classList.add('running');
                statusText.textContent = '🟢 System Running';
            } else {
                statusBadge.classList.remove('running');
                statusText.textContent = '🔴 System Stopped';
            }

            // Update learning phase
            const phase = data.system.learning_phase || 'Idle';
            document.getElementById('learning-phase').textContent = phase;
        }
    } catch (error) {
        console.error('Error loading system status:', error);
    }
}

// Load learning progress
async function loadLearningProgress() {
    try {
        const response = await fetch('/api/learning-progress');
        const data = await response.json();

        if (data.status === 'success') {
            // Update stats
            document.getElementById('predictions-total').textContent = data.predictions_total.toLocaleString();
            document.getElementById('predictions-completed').textContent = data.predictions_completed.toLocaleString();
            
            // Update accuracy
            const accuracy = (data.starter_accuracy * 100).toFixed(1);
            document.getElementById('current-accuracy').textContent = accuracy + '%';
            
            // Update progress bar
            const progressBar = document.getElementById('starter-accuracy-bar');
            progressBar.style.width = accuracy + '%';
            document.getElementById('starter-accuracy-value').textContent = accuracy + '%';

            // Update recommendations
            const recList = document.getElementById('recommendations');
            if (data.recommendations && data.recommendations.length > 0) {
                recList.innerHTML = '<ul>' + 
                    data.recommendations.map(rec => `<li>${rec}</li>`).join('') + 
                    '</ul>';
            } else {
                recList.innerHTML = '<p class="loading">System learning from data...</p>';
            }
        }
    } catch (error) {
        console.error('Error loading learning progress:', error);
    }
}

// Load metrics
async function loadMetrics() {
    try {
        const response = await fetch('/api/metrics');
        const data = await response.json();

        if (data.status === 'success' && data.metrics.length > 0) {
            // Update accuracy chart
            const labels = [];
            const accuracies = [];

            data.metrics.forEach((metric, index) => {
                const time = new Date(metric.timestamp);
                labels.push(time.toLocaleTimeString());
                accuracies.push((metric.accuracy * 100).toFixed(1));
            });

            if (accuracyChart) {
                accuracyChart.data.labels = labels;
                accuracyChart.data.datasets[0].data = accuracies;
                accuracyChart.update();
            }

            // Update error analysis
            if (data.metrics.length > 0) {
                const lastMetric = data.metrics[data.metrics.length - 1];
                if (lastMetric.errors_by_stat) {
                    const stats = Object.keys(lastMetric.errors_by_stat);
                    const errors = stats.map(stat => 
                        lastMetric.errors_by_stat[stat].mae || 0
                    );

                    if (errorChart) {
                        errorChart.data.labels = stats;
                        errorChart.data.datasets[0].data = errors;
                        errorChart.update();
                    }
                }
            }
        }
    } catch (error) {
        console.error('Error loading metrics:', error);
    }
}

// Load system information
async function loadSystemInfo() {
    try {
        const response = await fetch('/api/system-info');
        const data = await response.json();

        if (data.status === 'success') {
            // Update components
            const componentsList = document.getElementById('components-list');
            componentsList.innerHTML = '';
            Object.entries(data.components).forEach(([key, value]) => {
                const li = document.createElement('li');
                li.textContent = `${key}: ${value}`;
                componentsList.appendChild(li);
            });

            // Update data paths
            const pathsList = document.getElementById('data-paths');
            pathsList.innerHTML = '';
            Object.entries(data.data_paths).forEach(([key, value]) => {
                const li = document.createElement('li');
                li.textContent = value;
                li.title = key;
                pathsList.appendChild(li);
            });
        }
    } catch (error) {
        console.error('Error loading system info:', error);
    }
}

// Control functions
async function startSystem() {
    try {
        const btn = event.target.closest('.btn');
        btn.disabled = true;
        btn.textContent = '⏳ Starting...';

        const response = await fetch('/api/controls/start', { method: 'POST' });
        const data = await response.json();

        if (data.status === 'success') {
            showNotification('✓ System started successfully', 'success');
            setTimeout(() => {
                loadSystemStatus();
                btn.disabled = false;
                btn.innerHTML = '<span class="btn-icon">▶️</span> Start System';
            }, 1000);
        } else {
            showNotification('✗ Failed to start system', 'error');
            btn.disabled = false;
            btn.innerHTML = '<span class="btn-icon">▶️</span> Start System';
        }
    } catch (error) {
        console.error('Error starting system:', error);
        showNotification('✗ Error starting system', 'error');
        event.target.closest('.btn').disabled = false;
    }
}

async function stopSystem() {
    try {
        const btn = event.target.closest('.btn');
        btn.disabled = true;
        btn.textContent = '⏳ Stopping...';

        const response = await fetch('/api/controls/stop', { method: 'POST' });
        const data = await response.json();

        if (data.status === 'success') {
            showNotification('✓ System stopped', 'success');
            setTimeout(() => {
                loadSystemStatus();
                btn.disabled = false;
                btn.innerHTML = '<span class="btn-icon">⏹️</span> Stop System';
            }, 1000);
        } else {
            showNotification('✗ Failed to stop system', 'error');
            btn.disabled = false;
            btn.innerHTML = '<span class="btn-icon">⏹️</span> Stop System';
        }
    } catch (error) {
        console.error('Error stopping system:', error);
        showNotification('✗ Error stopping system', 'error');
        event.target.closest('.btn').disabled = false;
    }
}

async function runBacktest() {
    try {
        const btn = event.target.closest('.btn');
        btn.disabled = true;
        btn.textContent = '⏳ Running...';

        const response = await fetch('/api/controls/backtest', { method: 'POST' });
        const data = await response.json();

        if (data.status === 'success') {
            showNotification('✓ Backtest started (running in background)', 'success');
            setTimeout(() => {
                btn.disabled = false;
                btn.innerHTML = '<span class="btn-icon">📈</span> Run Backtest';
            }, 2000);
        } else {
            showNotification('✗ Failed to start backtest', 'error');
            btn.disabled = false;
            btn.innerHTML = '<span class="btn-icon">📈</span> Run Backtest';
        }
    } catch (error) {
        console.error('Error running backtest:', error);
        showNotification('✗ Error running backtest', 'error');
        event.target.closest('.btn').disabled = false;
    }
}

async function runLearning() {
    try {
        const btn = event.target.closest('.btn');
        btn.disabled = true;
        btn.textContent = '⏳ Running...';

        const response = await fetch('/api/controls/learn', { method: 'POST' });
        const data = await response.json();

        if (data.status === 'success') {
            showNotification('✓ Learning cycle started (running in background)', 'success');
            setTimeout(() => {
                btn.disabled = false;
                btn.innerHTML = '<span class="btn-icon">🧠</span> Run Learning';
            }, 2000);
        } else {
            showNotification('✗ Failed to start learning', 'error');
            btn.disabled = false;
            btn.innerHTML = '<span class="btn-icon">🧠</span> Run Learning';
        }
    } catch (error) {
        console.error('Error running learning:', error);
        showNotification('✗ Error running learning', 'error');
        event.target.closest('.btn').disabled = false;
    }
}

// Utility functions
function showNotification(message, type) {
    // Simple toast notification (you can enhance this)
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 15px 25px;
        background: ${type === 'success' ? '#2ca02c' : '#d62728'};
        color: white;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        font-weight: 600;
        z-index: 9999;
        animation: slideIn 0.3s ease;
    `;
    notification.textContent = message;
    document.body.appendChild(notification);

    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

function updateLastUpdate() {
    const now = new Date();
    document.getElementById('last-update').textContent = now.toLocaleTimeString();
}

// Add animation styles to head
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(400px);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(400px);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);

// Page visibility handling
document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
        clearInterval(refreshInterval);
    } else {
        // Resume refresh on 10s interval
        refreshInterval = setInterval(function() {
            loadSystemStatus();
            loadLearningProgress();
            loadMetrics();
            updateLastUpdate();
        }, 10000);
        
        // Immediate refresh when tab becomes visible
        loadSystemStatus();
        loadLearningProgress();
        loadMetrics();
    }
});

console.log('✓ JavaScript loaded');
