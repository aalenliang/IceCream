//
//  SyncEngine.swift
//  IceCream
//
//  Created by 蔡越 on 08/11/2017.
//

import CloudKit

/// SyncEngine talks to CloudKit directly.
/// Logically,
/// 1. it takes care of the operations of **CKDatabase**
/// 2. it handles all of the CloudKit config stuffs, such as subscriptions
/// 3. it hands over CKRecordZone stuffs to SyncObject so that it can have an effect on local Realm Database

public final class SyncEngine {
    
    private let databaseManager: DatabaseManager
    
    public convenience init(objects: [Syncable], databaseScope: CKDatabase.Scope = .private, container: CKContainer = .default(), callback: @escaping (SyncEngine) -> Void) {
        switch databaseScope {
        case .private:
            let privateDatabaseManager = PrivateDatabaseManager(objects: objects, container: container)
            self.init(databaseManager: privateDatabaseManager, callback: callback)
        case .public:
            let publicDatabaseManager = PublicDatabaseManager(objects: objects, container: container)
            self.init(databaseManager: publicDatabaseManager, callback: callback)
        default:
            fatalError("Shared database scope is not supported yet")
        }
    }
    
    private init(databaseManager: DatabaseManager, callback: @escaping (SyncEngine) -> Void) {
        self.databaseManager = databaseManager
        setup(callback)
    }
    
    private func setup(_ callback: @escaping (SyncEngine) -> Void) {
        databaseManager.prepare()
        databaseManager.container.accountStatus { [weak self] (status, error) in
            guard let self = self else { return }
            switch status {
            case .available:
                // self.databaseManager.registerLocalDatabase()
                self.databaseManager.createCustomZonesIfAllowed()
                // self.databaseManager.fetchChangesInDatabase(callback)
                self.databaseManager.resumeLongLivedOperationIfPossible()
                // self.databaseManager.startObservingRemoteChanges()
                // self.databaseManager.startObservingTermination()
                self.databaseManager.createDatabaseSubscriptionIfHaveNot()
                callback(self)
            case .noAccount, .restricted:
                guard self.databaseManager is PublicDatabaseManager else { break }
                // self.databaseManager.fetchChangesInDatabase(callback)
                self.databaseManager.resumeLongLivedOperationIfPossible()
                // self.databaseManager.startObservingRemoteChanges()
                // self.databaseManager.startObservingTermination()
                self.databaseManager.createDatabaseSubscriptionIfHaveNot()
                callback(self)
            case .couldNotDetermine:
                break
            @unknown default:
                break
            }
        }
    }
    
}

// MARK: Public Method
extension SyncEngine {
    
    /// Fetch data on the CloudKit and merge with local
    ///
    /// - Parameter completionHandler: Supported in the `privateCloudDatabase` when the fetch data process completes, completionHandler will be called. The error will be returned when anything wrong happens. Otherwise the error will be `nil`.
    public func pull(completionHandler: ((Error?) -> Void)? = nil) {
        databaseManager.fetchChangesInDatabase(completionHandler)
    }
    
    /// Push all existing local data to CloudKit
    /// You should NOT to call this method too frequently
    public func pushAll() {
        databaseManager.syncObjects.forEach { $0.pushLocalObjectsToCloudKit() }
    }

    public func push(objects: [CKRecordConvertible], completionHandler: ((Error?) -> Void)? = nil) {
        let recordsToStore: [CKRecord] = objects.filter { !$0.isDeleted }.map { $0.record }
        let recordsIDsToDelete: [CKRecord.ID] = objects.filter { $0.isDeleted }.map { $0.recordID }
        databaseManager.syncRecordsToCloudKit(recordsToStore: recordsToStore, recordIDsToDelete: recordsIDsToDelete, completion: completionHandler)
    }

    public func startObservingLocalAndRemoteChanges() {
        stopObservingLocalAndRemoteChanges() // Avoid add observer for multiple times.

        if self.databaseManager is PrivateDatabaseManager {
            databaseManager.registerLocalDatabase()
        }
        databaseManager.startObservingRemoteChanges()
        databaseManager.startObservingTermination()
    }

    public func stopObservingLocalAndRemoteChanges() {
        if self.databaseManager is PrivateDatabaseManager {
            databaseManager.unregisterLocalDatabase()
        }
        databaseManager.stopObservingRemoteChanges()
        databaseManager.stopObservingTermination()
    }

    public func partiallyPauseSync() {
        databaseManager.syncObjects.forEach { $0.pause() }
    }

    public func resumeSync() {
        databaseManager.syncObjects.forEach { $0.resume() }
    }
}

public enum Notifications: String, NotificationName {
    case cloudKitDataDidChangeRemotely
}

public enum IceCreamKey: String {
    /// Tokens
    case databaseChangesTokenKey
    case zoneChangesTokenKey
    
    /// Flags
    case subscriptionIsLocallyCachedKey
    case hasCustomZoneCreatedKey
    
    var value: String {
        return "icecream.keys." + rawValue
    }
}

/// Dangerous part:
/// In most cases, you should not change the string value cause it is related to user settings.
/// e.g.: the cloudKitSubscriptionID, if you don't want to use "private_changes" and use another string. You should remove the old subsription first.
/// Or your user will not save the same subscription again. So you got trouble.
/// The right way is remove old subscription first and then save new subscription.
public enum IceCreamSubscription: String, CaseIterable {
    case cloudKitPrivateDatabaseSubscriptionID = "private_changes"
    case cloudKitPublicDatabaseSubscriptionID = "cloudKitPublicDatabaseSubcriptionID"
    
    var id: String {
        return rawValue
    }
    
    public static var allIDs: [String] {
        return IceCreamSubscription.allCases.map { $0.rawValue }
    }
}
