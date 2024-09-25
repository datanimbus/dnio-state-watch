const { v4: uuidv4 } = require('uuid');

const COLLECTION_NAME = 'processState';

class StateManager {
    constructor(dbConnection, logger) {
        this.db = dbConnection; // Use the specific connection passed
        this.logger = logger; // Use the provided logger for logging
        this.State = this._createStateModel();
        this.FINAL_STATUSES = ['completed', 'failed', 'archived']; // Customize as needed
    }

    // Private method to define the State model using the specific connection
    _createStateModel() {
        const stateSchema = new this.db.Schema({
            id: { type: String, required: true, unique: true },
            data: { type: Object, required: true },
            status: { type: String, required: true },
            updatedAt: { type: Date, default: Date.now }
        });

        return this.db.model('State', stateSchema);
    }

    // Create a new state
    async createState(data) {
        const state = new this.State({
            id: uuidv4(),
            data: data,
            status: 'pending'
        });

        await state.save();

        return state.id;
    }

    // Update the state
    async updateState(stateId, data) {
        const state = await this.State.findOneAndUpdate(
            { id: stateId },
            { $set: { data: data.data, status: data.status, updatedAt: Date.now() } },
            { new: true }
        );
        if (!state) throw new Error('State not found');
        return state;
    }

    // Subscribe to state changes for a specific stateId
    onStateChange(stateId, callback) {
        // Listening to changes in the 'processState' collection
        const changeStream = this.db.collection(COLLECTION_NAME).watch();

        changeStream.on('change', (change) => {
            if (change.operationType === 'update' && change.documentKey._id === stateId) {
                this.logger.info(`State with ID ${stateId} has been updated.`);

                // Fetch the updated document
                this.db.collection(COLLECTION_NAME).findOne({ _id: stateId }, (err, updatedState) => {
                    if (err) {
                        this.logger.error('Error fetching updated state:', err);
                        return;
                    }
                    // Trigger the callback with the updated state
                    callback(updatedState);
                });
            }
        });

        this.logger.info(`Listening for changes to state with ID: ${stateId}`);
    }

    // Clean up states that have a final status
    async cleanUp() {
        try {
            const result = await this.State.deleteMany({ status: { $in: this.FINAL_STATUSES } });
            this.logger.info(`Cleanup complete: ${result.deletedCount} state(s) removed.`);
        } catch (error) {
            this.logger.error('Error during cleanup:', error);
        }
    }
}

module.exports = StateManager;